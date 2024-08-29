package spool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modfin/brev/pkg/zid"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
	"io"
	"strings"
	"sync"
	"time"
)

type Config struct {
}

const log string = "log"
const canonical string = "canonical"

const queue string = "queue"
const processing string = "processing"
const sent string = "sent"
const failed string = "failed"
const retry string = "retry"

var statuses = []string{queue, processing, sent, failed, retry}
var catagories = []string{log, canonical, queue, processing, sent, failed, retry}

// TODO replace with a interface of an FS enabling the use of eg s3 and gcs instead of local disk

type Spooler interface {
	Stop(ctx context.Context) error
	Enqueue(job []Job, email io.Reader) error
	Queue() <-chan *Job

	//Dequeue(eid xid.ID) (*Job, error)
	//Succeed(eid xid.ID) error
	//Fail(eid xid.ID) error
	//Logf(eid xid.ID, format string, args ...interface{}) error
}

type Job struct {
	TID  string   `json:"tid"`
	EID  zid.ID   `json:"eid"`
	From string   `json:"from"`
	Rcpt []string `json:"rcpt"`

	Try       int       `json:"try"`
	NotBefore time.Time `json:"not-before"`

	LocalName string `json:"local_name"`

	spool *Spool
}

func (j *Job) Status(tid string) (string, error) {
	if j.spool == nil {
		return "", fmt.Errorf("job has not been initialized and has no reference to spool")
	}
	return j.spool.Status(tid)
}

func (j *Job) Requeue() error {
	if j.spool == nil {
		return fmt.Errorf("job has not been initialized and has no reference to spool")
	}
	return j.spool.Requeue(j.TID)
}

func (j *Job) Fail() error {
	if j.spool == nil {
		return fmt.Errorf("job has not been initialized and has no reference to spool")
	}
	return j.spool.Move(j.TID, processing, failed)
}

func (j *Job) Success() error {
	if j.spool == nil {
		return fmt.Errorf("job has not been initialized and has no reference to spool")
	}
	return j.spool.Move(j.TID, processing, sent)
}

func (j *Job) Reader() (io.ReadCloser, error) {
	if j.spool == nil {
		return nil, fmt.Errorf("job has not been initialized and has no reference to spool")
	}

	eml, err := j.spool.fs.OpenReader(canonical, j.TID)
	if err != nil {
		return nil, fmt.Errorf("could not open eml file: %w", err)
	}
	return eml, nil
}
func (j *Job) WriteTo(w io.Writer) (n int64, err error) {
	if j.spool == nil {
		return 0, fmt.Errorf("job has not been initialized and has no reference to spool")
	}
	r, err := j.Reader()
	if err != nil {
		return 0, fmt.Errorf("could not get reader: %w", err)
	}
	return io.Copy(w, r)
}

func (j *Job) Logf(format string, args ...any) error {
	return j.spool.Logf(j.TID, format, args...)
}
func (j *Job) Retry() error {
	if j.spool == nil {
		return fmt.Errorf("job has not been initialized and has no reference to spool")
	}

	if j.NotBefore.IsZero() {
		return errors.New("job has no retry time")
	}

	err := j.spool.UpdateJob(j)
	if err != nil {
		return fmt.Errorf("could not update job: %w", err)
	}

	j.spool.log.WithField("tid", j.TID).Debugf("retry; will retry in %s", j.NotBefore.Sub(time.Now()).Truncate(time.Second).String())

	// TODO could cause a raise condition between Status and Move since we dont lock
	status, err := j.spool.Status(j.TID)
	if err != nil {
		return fmt.Errorf("could not find any reference to job %s, %w", j.TID, err)
	}
	if status == queue {
		return fmt.Errorf("job %s is already in queue", j.TID)
	}
	if status == sent {
		return fmt.Errorf("job %s is already sent", j.TID)
	}
	if status == retry {
		return fmt.Errorf("job %s is already in retry", j.TID)
	}

	return j.spool.Move(j.TID, status, retry)
}

func New(config Config, lc *tools.Logger, fs SFS) (*Spool, error) {

	logger := lc.New("spool")

	s := &Spool{
		fs:    fs,
		cfg:   config,
		log:   logger,
		queue: make(chan *Job), // ensures there is a handover.
		ctx:   context.Background(),
		sig:   make(chan struct{}, 1),
	}

	s.start()
	return s, nil

}

type Spool struct {
	ctx    context.Context
	cancel func()

	log   *logrus.Logger
	cfg   Config
	queue chan *Job

	ostart sync.Once
	ostop  sync.Once

	sig chan struct{}
	fs  SFS
}

func (s *Spool) start() {
	s.ostart.Do(func() {
		s.ctx, s.cancel = context.WithCancel(compare.Coalesce(s.ctx, context.Background()))

		s.log.Infof("Starting spool using")

		s.sigEnqueue()
		go s.startProducer()
		go s.startRetryer()

	})
}

func (s *Spool) Queue() <-chan *Job {
	return s.queue
}

func (s *Spool) Stop(ctx context.Context) error {
	s.ostop.Do(func() {
		s.cancel()
		close(s.queue)
	})
	return nil
}

func (s *Spool) sigEnqueue() {
	select {
	case s.sig <- struct{}{}:
	default:
	}
}

func (s *Spool) startProducer() {

	s.log.Infof("walker; starting")
	for {

		select {
		case <-s.ctx.Done():
			s.log.Infof("walker; stopping")
			return
		case <-s.sig: // Should contain stuff if there is something waiting or to be checked if waiting
		case <-time.After(2 * time.Minute):
		}

		s.log.Debug("walker; start walking queue dir")
		s.fs.Walk(queue, func(tid string) {

			s.log.WithField("tid", tid).Debug("walker; dequeue job")
			job, err := s.Dequeue(tid)
			if err != nil {
				s.log.WithError(err).WithField("tid", tid).Errorf("could not dequeue %s", tid)
				return
			}
			s.log.WithField("tid", tid).Debug("walker; writing job to spool consumer")
			s.queue <- job // Blocking until the job is consumed
		})

	}
}

func (s *Spool) startRetryer() {
	s.log.Debug("retry-walker; starting")
	for {
		// Should contain stuff if there is something waiting or to be checked if waiting
		select {
		case <-s.ctx.Done():
			s.log.Infof("retry-walker; stopping")
			return
		case <-time.After(1 * time.Minute):
		}

		s.log.Debug("retry walker; start walking retry dir")
		s.fs.Walk(retry, func(tid string) {

			s.log.WithField("tid", tid).Debug("retry walker; checking job, opening file, %s", tid)

			data, err := ReadAll(retry, tid, s.fs)
			if err != nil {
				err = fmt.Errorf("could not read job file: %w", err)
				_ = s.Logf(tid, "[spool error] faild read retry email: %s", err.Error())
				s.log.WithError(err).WithField("tid", tid).Error("retry failed; could not read job file")
				return
			}

			job := &Job{}
			err = json.Unmarshal(data, &job)
			if err != nil {
				err = fmt.Errorf("could not unmarshal job, %w", err)
				_ = s.Logf(tid, "[spool error] faild to retry email: %s", err.Error())
				s.log.WithError(err).WithField("tid", tid).Error("retry failed; could not unmarshal job")
				return
			}

			if time.Now().After(job.NotBefore) {
				s.log.WithField("tid", tid).Debug("retry walker; requeueing job")
				err = s.Requeue(tid)
				if err != nil {
					_ = s.Logf(tid, "[spool error] faild to retry email: %s", err.Error())
					s.log.WithError(err).WithField("tid", tid).Error("retry failed; could not requeue job")
				}
			}
		})

	}
}

// Probably implment file locking
// https://github.com/juju/fslock
// https://github.com/juju/mutex

func (s *Spool) Enqueue(jobs []Job, email io.Reader) error {
	if len(jobs) == 0 {
		return fmt.Errorf("no jobs to enqueue")
	}
	eid := jobs[0].EID

	if !slicez.EveryBy(jobs, func(j Job) bool { return j.EID.String() == eid.String() }) {
		return fmt.Errorf("all jobs must have the same EID")
	}

	s.fs.Lock(eid.String())
	defer s.fs.Unlock(eid.String())

	var err error

	var cleanup []string
	undo := func() error {
		return errors.Join(slicez.Map(cleanup, func(file string) error {
			s.log.Warnf("failed to commit email, cleaning up %s", file)
			catagory, tid, _ := strings.Cut(file, ":")
			return s.fs.Remove(catagory, tid)
		})...)
	}

	//Sync since we want to leave as much garanees that we can that the mail has been committed
	f, err := s.fs.OpenWriter(canonical, eid.String()) // TODO does it work, should really be TID
	if err != nil {
		return fmt.Errorf("could not open email file writer for eid %s: %w", eid.String(), err)
	}

	cleanup = append(cleanup, fmt.Sprintf("%s:%s", canonical, eid.String()))
	_, err = io.Copy(f, email)
	if err != nil {
		return errors.Join(err, f.Close(), undo())
	}
	s.log.WithField("eid", eid.String()).Debugf("enqueue;written cannonical email ")

	err = f.Close()
	if err != nil {
		return errors.Join(err, undo())
	}

	for _, job := range jobs {
		fun := func() error {
			s.fs.Lock(job.TID)
			defer s.fs.Unlock(job.TID)

			head, err := json.Marshal(job)
			if err != nil {
				err = fmt.Errorf("could not encode head: %w", err)
				return errors.Join(err, undo())
			}

			cleanup = append(cleanup, fmt.Sprintf("%s:%s", queue, job.TID))

			f, err = s.fs.OpenWriter(queue, job.TID)
			if err != nil {
				err = fmt.Errorf("could not open job file: %w", err)
				return errors.Join(err, f.Close(), undo())
			}

			_, err = f.Write(head)
			if err != nil {
				err = fmt.Errorf("could not write head: %w", err)
				return errors.Join(err, f.Close(), undo())
			}

			err = f.Close()
			if err != nil {
				err = fmt.Errorf("could not close job file: %w", err)
				return errors.Join(err, undo())
			}
			s.log.WithField("tid", job.TID).Debugf("enqueue; email had been enqueued")
			_ = s.Logf(job.TID, "[spool] email has been written to disk and enqueued")
			_ = s.Logf(job.TID, "[spool] rcpt [%s]", strings.Join(job.Rcpt, " "))
			return nil
		}
		err = fun()
		if err != nil {
			return err
		}
	}
	s.sigEnqueue()
	return nil

}

func (s *Spool) Logf(tid string, format string, args ...interface{}) (err error) {

	s.fs.Lock(tid + ".log")
	defer s.fs.Unlock(tid + ".log")

	msg := struct {
		TS  string `json:"ts"`
		TID string `json:"tid"`
		MSG string `json:"msg"`
	}{
		TS:  time.Now().In(time.UTC).Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
		TID: tid,
		MSG: fmt.Sprintf(format, args...),
	}

	buff := bytes.Buffer{}
	encoder := json.NewEncoder(&buff)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(msg)
	if err != nil {
		return fmt.Errorf("could not encode message: %w", err)
	}
	return s.logf(tid, buff.Bytes())
}
func (s *Spool) logf(tid string, data []byte) (err error) {
	f, err := s.fs.OpenWriter(log, tid)
	if err != nil {
		return fmt.Errorf("could not open log file: %w", err)
	}
	defer f.Close()

	_, err = f.Write(bytes.TrimSpace(data))
	if err != nil {
		return fmt.Errorf("could not write log message: %w", err)
	}
	_, err = f.Write([]byte("\n"))
	return err

}

var ErrNotFound = errors.New("not found")

func (s *Spool) Dequeue(tid string) (*Job, error) {
	s.fs.Lock(tid)
	defer s.fs.Unlock(tid)

	//emldir := eid2dir(canonical, eid)
	//queuedir := eid2dir(queue, eid)
	//procdir := eid2dir(processing, eid)
	//jobFilename := tid2name(tid)

	var err error
	if !s.fs.Exist(queue, tid) {
		err = fmt.Errorf("could not find job file in queue dir, %s, err: %w", tid, ErrNotFound)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not find job file in queue dir")
		return nil, err
	}

	if !s.fs.Exist(canonical, tid) {
		err = fmt.Errorf("email file, %s, does not exist", tid)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; email file does not exist")
		return nil, err
	}

	err = s.move(tid, queue, processing)
	if err != nil {
		err = fmt.Errorf("could not rename job file: %w", err)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not rename job file")
		return nil, err
	}

	data, err := ReadAll(processing, tid, s.fs)
	if err != nil {
		err = fmt.Errorf("could not read job file: %w", err)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not read job file")
		return nil, err
	}

	var job Job

	err = json.Unmarshal(data, &job)
	if err != nil {
		err = fmt.Errorf("could not unmarshal job, %w", err)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not unmarshal job")
		return nil, err
	}
	job.spool = s

	_ = s.Logf(tid, "[spool] email had been dequeued for processing")
	s.log.WithField("tid", job.TID).Debugf("dequeue; email had been dequeued")
	return &job, nil
}

func (s *Spool) Requeue(tid string) error {
	s.fs.Lock(tid)
	defer s.fs.Unlock(tid)

	status, err := s.status(tid)
	if err != nil {
		return fmt.Errorf("could not find any reference to job %s, %w", tid, err)
	}
	if status == queue {
		return fmt.Errorf("job %s is already in queue", tid)
	}

	err = s.move(tid, status, queue)
	if err != nil {
		return fmt.Errorf("could not Move job file from %s to queu: %w", status, err)
	}

	// Inform the walker that there is a new job to be dequeued
	s.sigEnqueue()

	_ = s.Logf(tid, "[spool] email has been requeued, moved from '%s' to 'queue'", status)
	s.log.WithField("tid", tid).Debugf("succeed; email had been requeued")

	return nil
}

func (s *Spool) Status(tid string) (string, error) {
	s.fs.Lock(tid)
	defer s.fs.Unlock(tid)
	return s.status(tid)
}

func (s *Spool) status(tid string) (string, error) {
	for _, category := range statuses {
		if s.fs.Exist(category, tid) {
			return category, nil
		}
	}
	return "", fmt.Errorf("could not find any reference to job %s", tid)
}

func (s *Spool) Move(tid string, from string, to string) error {
	s.fs.Lock(tid)
	defer s.fs.Unlock(tid)

	return s.move(tid, from, to)
}
func (s *Spool) move(tid string, from string, to string) error {

	if !slicez.Contains(statuses, from) {
		return fmt.Errorf("invalid 'from' status: %s", from)
	}
	if !slicez.Contains(statuses, to) {
		return fmt.Errorf("invalid 'to' status: %s", to)
	}

	err := s.fs.Move(tid, from, to)

	if err != nil {
		return fmt.Errorf("could not Move job file from %s to %s: %w", from, to, err)
	}

	_ = s.Logf(tid, "[spool] email has marked as '%s', moved from '%s' to '%s'", to, from, to)
	s.log.WithField("tid", tid).Debugf("Move; email had moved from %s to %s", from, to)
	return nil
}

func (s *Spool) UpdateJob(j *Job) error {

	s.fs.Lock(j.TID)
	defer s.fs.Unlock(j.TID)
	status, err := s.status(j.TID)
	if err != nil {
		return fmt.Errorf("could not find any reference to job %s, %w", j.EID.String(), err)
	}

	head, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("could not encode head: %w", err)
	}

	f, err := s.fs.OpenWriter(status, j.TID)
	if err != nil {
		err = fmt.Errorf("could not open job file: %w", err)
		return errors.Join(err, f.Close())
	}

	_, err = f.Write(head)
	if err != nil {
		err = fmt.Errorf("could not write head: %w", err)
		return errors.Join(err, f.Close())
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("could not close job file: %w", err)
	}
	return nil

}

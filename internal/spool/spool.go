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
	"path/filepath"
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

	emlPath string
	spool   *Spool
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

	eml, err := j.spool.fs.OpenReader(j.emlPath)
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
		mu:    tools.NewKeyedMutex(),
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
	mu    *tools.KeyedMutex
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
		s.fs.Walk(queue, func(path string) {
			if filepath.Ext(path) != ".job" {
				return
			}
			base := filepath.Base(path)
			tid := strings.TrimSuffix(base, ".job")

			// TODO rexexp check for tid

			// TODO keep track of recently enqueued emails to avoid re-enqueuing or refactor to dequeue and not just send the reference
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
		s.fs.Walk(retry, func(path string) {
			if filepath.Ext(path) != ".job" {
				return
			}
			base := filepath.Base(path)
			tid := strings.TrimSuffix(base, ".job")
			// TODO rexexp check for tid

			s.log.WithField("tid", tid).Debug("retry walker; checking job, opening file, %s", path)

			data, err := ReadAll(path, s.fs)
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

	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	var err error

	var cleanup []string
	undo := func() error {
		return errors.Join(slicez.Map(cleanup, func(path string) error {
			s.log.Warnf("failed to commit email, cleaning up %s", path)
			return s.fs.Remove(path)
		})...)
	}

	dir := eid2dir(canonical, eid)

	filename := fmt.Sprintf("%s.eml", eid)
	emailPath := filepath.Join(dir, filename)

	//Sync since we want to leave as much garanees that we can that the mail has been committed
	f, err := s.fs.OpenWrite(emailPath, false)
	if err != nil {
		return err
	}

	cleanup = append(cleanup, emailPath)
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

		head, err := json.Marshal(job)
		if err != nil {
			err = fmt.Errorf("could not encode head: %w", err)
			return errors.Join(err, undo())
		}

		dir = eid2dir(queue, job.EID)
		filename = tid2name(job.TID)
		jobPath := filepath.Join(dir, filename)
		cleanup = append(cleanup, jobPath)

		f, err = s.fs.OpenWrite(jobPath, false)
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

	}
	s.sigEnqueue()
	return nil

}

func (s *Spool) Logf(tid string, format string, args ...interface{}) (err error) {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(tid + ".log")
	defer s.mu.Unlock(tid + ".log")

	msg := struct {
		TS  string `json:"ts"`
		EID string `json:"eid"`
		MSG string `json:"msg"`
	}{
		TS:  time.Now().In(time.UTC).Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
		EID: eid.String(),
		MSG: fmt.Sprintf(format, args...),
	}

	buff := bytes.Buffer{}
	encoder := json.NewEncoder(&buff)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(msg)
	if err != nil {
		return fmt.Errorf("could not encode message: %w", err)
	}
	return s.lograw(tid, buff.Bytes())
}
func (s *Spool) lograw(tid string, data []byte) (err error) {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}

	dir := eid2dir(log, eid)
	filename := tid2name(tid)
	path := filepath.Join(dir, filename)

	f, err := s.fs.OpenWrite(path, true)
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
	eid, err := tid2eid(tid)
	if err != nil {
		return nil, fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	emldir := eid2dir(canonical, eid)
	queuedir := eid2dir(queue, eid)
	procdir := eid2dir(processing, eid)

	jobFilename := tid2name(tid)

	if !s.fs.Exist(filepath.Join(queuedir, jobFilename)) {
		err = fmt.Errorf("could not find job file in queue dir, %s, err: %w", filepath.Join(queuedir, jobFilename), ErrNotFound)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not find job file in queue dir")
		return nil, err
	}

	err = s.move(tid, queue, processing)
	if err != nil {
		err = fmt.Errorf("could not rename job file: %w", err)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not rename job file")
		return nil, err
	}

	data, err := ReadAll(filepath.Join(procdir, jobFilename), s.fs)
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
	job.emlPath = filepath.Join(emldir, fmt.Sprintf("%s.eml", eid))
	if !s.fs.Exist(job.emlPath) {
		err = fmt.Errorf("email file, %s, does not exist", job.emlPath)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; email file does not exist")
		return nil, err
	}

	_ = s.Logf(tid, "[spool] email had been dequeued for processing")
	s.log.WithField("tid", job.TID).Debugf("dequeue; email had been dequeued")
	return &job, nil
}

func (s *Spool) Requeue(tid string) error {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	status, err := s.status(tid)
	if err != nil {
		return fmt.Errorf("could not find any reference to job %s, %w", eid.String(), err)
	}
	if status == queue {
		return fmt.Errorf("job %s is already in queue", eid.String())
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
	eid, err := tid2eid(tid)
	if err != nil {
		return "", fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())
	return s.status(tid)
}

func (s *Spool) status(tid string) (string, error) {
	eid, err := tid2eid(tid)
	if err != nil {
		return "", fmt.Errorf("could not parse xid: %w", err)
	}

	for _, dir := range statuses {
		dirpath := eid2dir(dir, eid)
		path := filepath.Join(dirpath, tid2name(tid))
		if s.fs.Exist(path) {
			return dir, nil
		}
	}
	return "", fmt.Errorf("could not find any reference to job %s", eid.String())
}

func (s *Spool) Move(tid string, from string, to string) error {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	return s.move(tid, from, to)
}
func (s *Spool) move(tid string, from string, to string) error {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}

	oldStatus := from
	newStatus := to
	from = eid2dir(oldStatus, eid)
	to = eid2dir(newStatus, eid)
	jobFilename := tid2name(tid)

	err = s.fs.Rename(filepath.Join(from, jobFilename), filepath.Join(to, jobFilename))
	if err != nil {
		return fmt.Errorf("could not Move job file from %s to %s: %w", oldStatus, newStatus, err)
	}

	_ = s.Logf(tid, "[spool] email has marked as '%s', moved from '%s' to '%s'", newStatus, oldStatus, newStatus)
	s.log.WithField("tid", tid).Debugf("Move; email had moved from %s to %s", oldStatus, newStatus)
	return nil
}

func (s *Spool) UpdateJob(j *Job) error {

	s.mu.Lock(j.EID.String())
	defer s.mu.Unlock(j.EID.String())
	status, err := s.status(j.TID)
	if err != nil {
		return fmt.Errorf("could not find any reference to job %s, %w", j.EID.String(), err)
	}

	head, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("could not encode head: %w", err)
	}

	dir := eid2dir(status, j.EID)

	filename := tid2name(j.TID)
	jobPath := filepath.Join(dir, filename)

	f, err := s.fs.OpenWrite(jobPath, false)
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

func tid2name(tid string) string {
	return fmt.Sprintf("%s.job", tid)
}

func tid2eid(tid string) (zid.ID, error) {
	eid, _, err := zid.FromTID(tid)
	if err != nil {
		return zid.ID{}, fmt.Errorf("could not parse xid from tid: %w", err)
	}
	return eid, nil
}

func eid2dir(catalog string, eid zid.ID) string {
	t := eid.Time()
	day := t.In(time.UTC).Format("2006-01-02")
	hour := t.In(time.UTC).Format("15")
	return filepath.Join(catalog, day, hour)
}

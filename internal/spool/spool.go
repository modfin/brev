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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Dir string `cli:"spool-dir"`
}

const log string = "log"
const canonical string = "canonical"

const queue string = "queue"
const processing string = "processing"
const sent string = "sent"
const failed string = "failed"
const retry string = "retry"

var statuses = []string{queue, processing, sent, failed, retry}

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
	return j.spool.Fail(j.TID)
}

func (j *Job) Success() error {
	if j.spool == nil {
		return fmt.Errorf("job has not been initialized and has no reference to spool")
	}
	return j.spool.Succeed(j.TID)
}

func (j *Job) Reader() (io.ReadSeekCloser, error) {
	if j.spool == nil {
		return nil, fmt.Errorf("job has not been initialized and has no reference to spool")
	}

	eml, err := os.OpenFile(j.emlPath, os.O_RDONLY, 0644)
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

	return j.spool.Retry(j.TID)
}

func New(config Config, lc *tools.Logger) (*Spool, error) {

	logger := lc.New("spool")

	for _, dir := range []string{log, queue, processing, sent, failed, retry} {
		var err error
		err = os.MkdirAll(filepath.Join(config.Dir, dir), 0755)
		if err != nil {
			return nil, fmt.Errorf("could not create directory: %w", err)
		}
	}

	s := &Spool{
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
}

func (s *Spool) start() {
	s.ostart.Do(func() {
		s.ctx, s.cancel = context.WithCancel(compare.Coalesce(s.ctx, context.Background()))
		a, _ := filepath.Abs(s.cfg.Dir)
		s.log.Infof("Starting spool using %s", compare.Coalesce(a, s.cfg.Dir))

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
	dir := filepath.Join(s.cfg.Dir, queue)

	s.log.Debug("walker; starting")
	for {

		<-s.sig // Should contain stuff if there is something waiting or to be checked if waiting
		s.log.Debug("walker; start walking queue dir")
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if filepath.Ext(path) != ".job" {
				return nil
			}
			base := filepath.Base(path)
			tid := strings.TrimSuffix(base, ".job")
			// TODO rexexp check for tid
			if err != nil {
				s.log.WithError(err).Errorf("could not parse xid from %s", path)
				return err
			}

			// TODO keep track of recently enqueued emails to avoid re-enqueuing or refactor to dequeue and not just send the reference
			s.log.WithField("tid", tid).Debug("walker; dequeue job")
			job, err := s.Dequeue(tid)
			if err != nil {
				s.log.WithError(err).WithField("tid", tid).Errorf("could not dequeue %s", tid)
				return err
			}
			s.log.WithField("tid", tid).Debug("walker; writing job to spool consumer")
			s.queue <- job // Blocking until the job is consumed

			return nil
		})

	}
}

func (s *Spool) startRetryer() {
	dir := filepath.Join(s.cfg.Dir, retry)
	s.log.Debug("retry walker; starting")
	for {

		<-time.After(time.Minute) // Should contain stuff if there is something waiting or to be checked if waiting
		s.log.Debug("retry walker; start walking retry dir")
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if filepath.Ext(path) != ".job" {
				return nil
			}
			base := filepath.Base(path)
			tid := strings.TrimSuffix(base, ".job")
			// TODO rexexp check for tid
			if err != nil {
				s.log.WithError(err).Errorf("could not parse xid from %s", path)
				return err
			}

			// TODO keep track of recently enqueued emails to avoid re-enqueuing or refactor to dequeue and not just send the reference
			s.log.WithField("tid", tid).Debug("retry walker; checking job")

			data, err := os.ReadFile(path)
			if err != nil {
				err = fmt.Errorf("could not read job file: %w", err)
				_ = s.Logf(tid, "[spool error] faild read retry email: %s", err.Error())
				s.log.WithError(err).WithField("tid", tid).Error("retry failed; could not read job file")
				return nil
			}
			job := &Job{}
			err = json.Unmarshal(data, &job)
			if err != nil {
				err = fmt.Errorf("could not unmarshal job, %w", err)
				_ = s.Logf(tid, "[spool error] faild to retry email: %s", err.Error())
				s.log.WithError(err).WithField("tid", tid).Error("retry failed; could not unmarshal job")
				return nil
			}

			if time.Now().After(job.NotBefore) {
				s.log.WithField("tid", tid).Debug("retry walker; requeueing job")
				err = s.Requeue(tid)
				if err != nil {
					_ = s.Logf(tid, "[spool error] faild to retry email: %s", err.Error())
					s.log.WithError(err).WithField("tid", tid).Error("retry failed; could not requeue job")
				}
			}

			return nil
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
			return os.Remove(path)
		})...)
	}

	dir := eid2dir(s.cfg.Dir, canonical, eid)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	filename := fmt.Sprintf("%s.eml", eid)
	emailPath := filepath.Join(dir, filename)

	//Sync since we want to leave as much garanees that we can that the mail has been committed
	f, err := os.OpenFile(emailPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
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

		dir = eid2dir(s.cfg.Dir, queue, job.EID)
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			err = fmt.Errorf("could not create directory: %w", err)
			return errors.Join(err, undo())
		}

		filename = tid2name(job.TID)
		jobPath := filepath.Join(dir, filename)
		cleanup = append(cleanup, jobPath)

		f, err = os.OpenFile(jobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
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

	dir := eid2dir(s.cfg.Dir, log, eid)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	filename := tid2name(tid)
	path := filepath.Join(dir, filename)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
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

	emldir := eid2dir(s.cfg.Dir, canonical, eid)
	queuedir := eid2dir(s.cfg.Dir, queue, eid)
	procdir := eid2dir(s.cfg.Dir, processing, eid)

	jobFilename := tid2name(tid)

	if !fileExists(filepath.Join(queuedir, jobFilename)) {
		err = fmt.Errorf("could not find job file in queue dir, %s, err: %w", filepath.Join(queuedir, jobFilename), ErrNotFound)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not find job file in queue dir")
		return nil, err
	}

	err = os.MkdirAll(procdir, 0755)
	if err != nil {
		err = fmt.Errorf("could not create processing directory: %w", err)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not create processing directory")
		return nil, err
	}
	err = os.Rename(filepath.Join(queuedir, jobFilename), filepath.Join(procdir, jobFilename))
	if err != nil {
		err = fmt.Errorf("could not rename job file: %w", err)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; could not rename job file")
		return nil, err
	}

	data, err := os.ReadFile(filepath.Join(procdir, jobFilename))
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
	if !fileExists(job.emlPath) {
		err = fmt.Errorf("email file, %s, does not exist", job.emlPath)
		_ = s.Logf(tid, "[spool error] faild to dequeue email: %s", err.Error())
		s.log.WithError(err).WithField("tid", tid).Error("dequeue failed; email file does not exist")
		return nil, err
	}

	_ = s.Logf(tid, "[spool] email had been dequeued for processing")
	s.log.WithField("tid", job.TID).Debugf("dequeue; email had been dequeued")
	return &job, nil
}

func (s *Spool) Succeed(tid string) error {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	procdir := eid2dir(s.cfg.Dir, processing, eid)
	sentdir := eid2dir(s.cfg.Dir, sent, eid)

	jobFilename := tid2name(tid)

	if !fileExists(filepath.Join(procdir, jobFilename)) {
		return fmt.Errorf("could not find job file in process dir, %s", filepath.Join(procdir, jobFilename))
	}

	err = os.MkdirAll(sentdir, 0775)
	if err != nil {
		return fmt.Errorf("could not create sent dir, %w", err)
	}

	err = os.Rename(filepath.Join(procdir, jobFilename), filepath.Join(sentdir, jobFilename))
	if err != nil {
		return fmt.Errorf("could not move job file to sent: %w", err)
	}

	_ = s.Logf(tid, "[spool] email had been successfully sent")
	s.log.WithField("tid", tid).Debugf("succeed; email had been marked sent")

	return nil
}

func (s *Spool) Fail(tid string) error {
	eid, err := tid2eid(tid)
	if err != nil {
		return fmt.Errorf("could not parse xid: %w", err)
	}
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	procdir := eid2dir(s.cfg.Dir, processing, eid)
	faildir := eid2dir(s.cfg.Dir, failed, eid)

	jobFilename := tid2name(tid)

	if !fileExists(filepath.Join(procdir, jobFilename)) {
		return fmt.Errorf("could not find job file in process dir, %s", filepath.Join(procdir, jobFilename))
	}

	err = os.MkdirAll(faildir, 0775)
	if err != nil {
		return fmt.Errorf("could not create failed dir, %w", err)
	}

	err = os.Rename(filepath.Join(procdir, jobFilename), filepath.Join(faildir, jobFilename))
	if err != nil {
		return fmt.Errorf("could not move job file to sent: %w", err)
	}

	_ = s.Logf(tid, "[spool] email has failed to send")
	s.log.WithField("tid", tid).Debugf("fail; email had been marked failed")

	return nil
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

	from := eid2dir(s.cfg.Dir, status, eid)
	to := eid2dir(s.cfg.Dir, queue, eid)
	jobFilename := tid2name(tid)

	err = os.MkdirAll(to, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	err = os.Rename(filepath.Join(from, jobFilename), filepath.Join(to, jobFilename))
	if err != nil {
		return fmt.Errorf("could not move job file from %s to queu: %w", status, err)
	}

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
		dirpath := eid2dir(s.cfg.Dir, dir, eid)
		path := filepath.Join(dirpath, tid2name(tid))
		if fileExists(path) {
			return dir, nil
		}
	}
	return "", fmt.Errorf("could not find any reference to job %s", eid.String())
}

func (s *Spool) Retry(tid string) error {
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
	if status == sent {
		return fmt.Errorf("job %s is already sent", eid.String())
	}
	if status == retry {
		return fmt.Errorf("job %s is already in retry", eid.String())
	}

	from := eid2dir(s.cfg.Dir, status, eid)
	to := eid2dir(s.cfg.Dir, retry, eid)
	jobFilename := tid2name(tid)

	err = os.MkdirAll(to, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	err = os.Rename(filepath.Join(from, jobFilename), filepath.Join(to, jobFilename))
	if err != nil {
		return fmt.Errorf("could not move job file from %s to retry: %w", status, err)
	}

	_ = s.Logf(tid, "[spool] email has been requeued, moved from '%s' to 'retry'", status)
	s.log.WithField("tid", tid).Debugf("succeed; email had moved to retry")

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

	dir := eid2dir(s.cfg.Dir, status, j.EID)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	filename := tid2name(j.TID)
	jobPath := filepath.Join(dir, filename)

	f, err := os.OpenFile(jobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
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

func eid2dir(prefix string, catalog string, eid zid.ID) string {
	t := eid.Time()
	day := t.In(time.UTC).Format("2006-01-02")
	hour := t.In(time.UTC).Format("15")
	return filepath.Join(prefix, catalog, day, hour)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !errors.Is(err, os.ErrNotExist)
}

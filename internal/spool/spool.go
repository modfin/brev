package spool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/rs/xid"
	"github.com/sirupsen/logrus"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Spooler interface {
	Start() <-chan xid.ID
	Stop(ctx context.Context) error
	Enqueue(job Job, email io.Reader) error
	Dequeue(eid xid.ID) (*Job, ReadSeekCloserCloner, error)
	Succeed(eid xid.ID) error
	Fail(eid xid.ID) error
	Logf(eid xid.ID, format string, args ...interface{}) error
}

type Job struct {
	EID         xid.ID   `json:"eid"`
	MessageId   string   `json:"message_id"`
	From        string   `json:"from"`
	Rcpt        []string `json:"rcpt"`
	Fails       int      `json:"Fails,omitempty"`
	SuccessRcpt []string `json:"success_rcpt,omitempty"`
}

type Spool struct {
	ctx    context.Context
	cancel func()

	log   *logrus.Logger
	mu    *tools.KeyedMutex
	cfg   Config
	queue chan xid.ID

	start sync.Once
	stop  sync.Once

	sig chan struct{}
}

type ReadSeekCloserCloner interface {
	io.ReadSeekCloser
	Clone() (ReadSeekCloserCloner, error)
}

type readerfile struct {
	*os.File
}

func (f *readerfile) Clone() (ReadSeekCloserCloner, error) {
	ff, err := os.OpenFile(f.Name(), os.O_RDONLY, 0644)
	return &readerfile{ff}, err
}

func (s *Spool) Stop(ctx context.Context) error {
	s.stop.Do(func() {
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

	for {

		<-s.sig // Should contain stuff if there is something waiting or to be checked if waiting
		s.log.Debug("walking queue dir")
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if filepath.Ext(path) != ".job" {
				return nil
			}
			base := filepath.Base(path)
			base = strings.TrimSuffix(base, ".job")
			eid, err := xid.FromString(base)
			if err != nil {
				s.log.WithError(err).Errorf("could not parse xid from %s", path)
				return err
			}

			// TODO keep track of recently enqueued emails to avoid re-enqueuing or refactor to dequeue and not just send the reference

			s.log.Infof("enqueuing %s from %s to chan", eid.String(), dir)

			s.queue <- eid

			return nil
		})

	}
}

func (s *Spool) Start() <-chan xid.ID {
	s.start.Do(func() {
		s.ctx, s.cancel = context.WithCancel(compare.Coalesce(s.ctx, context.Background()))
		a, _ := filepath.Abs(s.cfg.Dir)
		s.log.Infof("Starting spool using %s", compare.Coalesce(a, s.cfg.Dir))

		s.sigEnqueue()
		go s.startProducer()

	})

	return s.queue
}

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

// Probably implment file locking
// https://github.com/juju/fslock
// https://github.com/juju/mutex

func New(config Config) (*Spool, error) {

	logger := logrus.New()
	logger.AddHook(tools.LoggerWho{Name: "spool"})

	var err error
	err = os.MkdirAll(filepath.Join(config.Dir, log), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}

	err = os.MkdirAll(filepath.Join(config.Dir, queue), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}
	err = os.MkdirAll(filepath.Join(config.Dir, processing), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}

	err = os.MkdirAll(filepath.Join(config.Dir, sent), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}

	err = os.MkdirAll(filepath.Join(config.Dir, retry), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}

	return &Spool{
		cfg:   config,
		log:   logger,
		mu:    tools.NewKeyedMutex(),
		queue: make(chan xid.ID), // ensures there is a handover.
		ctx:   context.Background(),
		sig:   make(chan struct{}, 1),
	}, nil
}

func (s *Spool) Enqueue(job Job, email io.Reader) error {
	s.mu.Lock(job.EID.String())
	defer s.mu.Unlock(job.EID.String())

	var err error

	dir := eid2dir(s.cfg.Dir, canonical, job.EID)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	filename := fmt.Sprintf("%s.eml", job.EID)
	emailPath := filepath.Join(dir, filename)

	//Sync since we want to leave as much garanees that we can that the mail has been committed
	f, err := os.OpenFile(emailPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, email)
	if err != nil {
		return errors.Join(err, f.Close(), os.Remove(emailPath))
	}
	err = f.Close()
	if err != nil {
		return errors.Join(err, os.Remove(emailPath))
	}

	head, err := json.Marshal(job)
	if err != nil {
		err = fmt.Errorf("could not encode head: %w", err)
		return errors.Join(err, os.Remove(emailPath))
	}

	dir = eid2dir(s.cfg.Dir, queue, job.EID)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		err = fmt.Errorf("could not create directory: %w", err)
		return errors.Join(err, os.Remove(emailPath))
	}

	filename = fmt.Sprintf("%s.job", job.EID)
	jobPath := filepath.Join(dir, filename)

	f, err = os.OpenFile(jobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		err = fmt.Errorf("could not open job file: %w", err)
		return errors.Join(err, f.Close(), os.Remove(emailPath), os.Remove(jobPath))
	}

	_, err = f.Write(head)
	if err != nil {
		err = fmt.Errorf("could not write head: %w", err)
		return errors.Join(err, f.Close(), os.Remove(emailPath), os.Remove(jobPath))
	}

	err = f.Close()
	if err != nil {
		err = fmt.Errorf("could not close job file: %w", err)
		return errors.Join(err, os.Remove(emailPath), os.Remove(jobPath))
	}

	s.sigEnqueue()

	return s.Logf(job.EID, "email has been written to disk and enqueued")

}

func (s *Spool) Logf(eid xid.ID, format string, args ...interface{}) (err error) {
	s.mu.Lock(eid.String() + ".log")
	defer s.mu.Unlock(eid.String() + ".log")
	msg := struct {
		TS  time.Time `json:"ts"`
		EID string    `json:"eid"`
		MSG string    `json:"msg"`
	}{
		TS:  time.Now().In(time.UTC).Truncate(time.Millisecond),
		EID: eid.String(),
		MSG: fmt.Sprintf(format, args...),
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not encode message: %w", err)
	}
	return s.lograw(eid, b)
}
func (s *Spool) lograw(eid xid.ID, data []byte) (err error) {

	dir := eid2dir(s.cfg.Dir, log, eid)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	filename := fmt.Sprintf("%s.log", eid.String())
	path := filepath.Join(dir, filename)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
	if err != nil {
		return fmt.Errorf("could not open log file: %w", err)
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("could not write log message: %w", err)
	}
	_, err = f.Write([]byte("\n"))
	return err

}

var ErrNotFound = errors.New("not found")

func (s *Spool) Dequeue(eid xid.ID) (*Job, ReadSeekCloserCloner, error) {
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	emldir := eid2dir(s.cfg.Dir, canonical, eid)
	queuedir := eid2dir(s.cfg.Dir, queue, eid)
	procdir := eid2dir(s.cfg.Dir, processing, eid)

	jobFilename := fmt.Sprintf("%s.job", eid)

	if !fileExists(filepath.Join(queuedir, jobFilename)) {
		return nil, nil, fmt.Errorf("could not find job file in queue dir, %s, err: %w", filepath.Join(queuedir, jobFilename), ErrNotFound)
	}

	emlFilename := fmt.Sprintf("%s.eml", eid)
	eml, err := os.OpenFile(filepath.Join(emldir, emlFilename), os.O_RDONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("could not open eml file: %w", err)
	}

	err = os.MkdirAll(procdir, 0755)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create processing directory: %w", err)
	}
	err = os.Rename(filepath.Join(queuedir, jobFilename), filepath.Join(procdir, jobFilename))
	if err != nil {
		return nil, nil, fmt.Errorf("could not rename job file: %w", err)
	}

	job, err := os.ReadFile(filepath.Join(procdir, jobFilename))
	if err != nil {
		return nil, nil, fmt.Errorf("could not read job file: %w", err)
	}

	var head Job
	err = json.Unmarshal(job, &head)
	if err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal job, %w", err)
	}

	_ = s.Logf(eid, "email had been deququed for processing")
	return &head, &readerfile{eml}, nil
}

func (s *Spool) Succeed(eid xid.ID) error {
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	procdir := eid2dir(s.cfg.Dir, processing, eid)
	sentdir := eid2dir(s.cfg.Dir, sent, eid)

	jobFilename := fmt.Sprintf("%s.job", eid)

	if !fileExists(filepath.Join(procdir, jobFilename)) {
		return fmt.Errorf("could not find job file in process dir, %s", filepath.Join(procdir, jobFilename))
	}

	err := os.MkdirAll(sentdir, 0775)
	if err != nil {
		return fmt.Errorf("could not create sent dir, %w", err)
	}

	err = os.Rename(filepath.Join(procdir, jobFilename), filepath.Join(sentdir, jobFilename))
	if err != nil {
		return fmt.Errorf("could not move job file to sent: %w", err)
	}

	_ = s.Logf(eid, "email had been successfully sent")

	return nil
}

func (s *Spool) Fail(eid xid.ID) error {
	s.mu.Lock(eid.String())
	defer s.mu.Unlock(eid.String())

	procdir := eid2dir(s.cfg.Dir, processing, eid)
	faildir := eid2dir(s.cfg.Dir, failed, eid)

	jobFilename := fmt.Sprintf("%s.job", eid)

	if !fileExists(filepath.Join(procdir, jobFilename)) {
		return fmt.Errorf("could not find job file in process dir, %s", filepath.Join(procdir, jobFilename))
	}

	err := os.MkdirAll(faildir, 0775)
	if err != nil {
		return fmt.Errorf("could not create failed dir, %w", err)
	}

	err = os.Rename(filepath.Join(procdir, jobFilename), filepath.Join(faildir, jobFilename))
	if err != nil {
		return fmt.Errorf("could not move job file to sent: %w", err)
	}

	_ = s.Logf(eid, "email has failed to send")

	return nil
}

func eid2dir(prefix string, catalog string, eid xid.ID) string {
	t := eid.Time()
	day := t.In(time.UTC).Format("2006-01-02")
	hour := t.In(time.UTC).Format("15")
	return filepath.Join(prefix, catalog, day, hour)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !errors.Is(err, os.ErrNotExist)
}

package spool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/envelope"
	"github.com/modfin/brev/smtpx/envelope/kim"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/rs/xid"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Spool struct {
	log *logrus.Logger
	cfg Config
}

func (s *Spool) Stop(ctx context.Context) error {
	return nil
}

func (s *Spool) Start() {
	a, _ := filepath.Abs(s.cfg.Dir)
	s.log.Infof("Starting spool using %s", compare.Coalesce(a, s.cfg.Dir))
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
	return &Spool{cfg: config, log: logger}, nil
}

type Job struct {
	EID         xid.ID   `json:"eid"`
	From        string   `json:"from"`
	Rcpt        []string `json:"rcpt"`
	Retries     int      `json:"retries"`
	SuccessRcpt []string `json:"success_rcpt"`
}

//func lock(f *os.File) (unlock func() error, err error) {
//	fd := int(f.Fd())
//	err = syscall.Flock(fd, syscall.LOCK_EX)
//
//	unlock = func() error {
//		return nil
//	}
//
//	if errors.Is(err, syscall.EAGAIN) { // TODO wait and see?
//		fmt.Println("File is already locked")
//	}
//	if err != nil {
//		return unlock, err
//	}
//
//	unlock = func() error {
//		return syscall.Flock(fd, syscall.LOCK_UN)
//	}
//	return unlock, nil
//}

func (s *Spool) Enqueue(e *brev.Email, signer *kim.Signer) error {

	var err error
	eid := e.Metadata.Id

	dir := eid2dir(s.cfg.Dir, canonical, eid)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	filename := fmt.Sprintf("%s.eml", eid)
	emailPath := filepath.Join(dir, filename)

	emlreader, err := envelope.Marshal(e, signer)
	if err != nil {
		return fmt.Errorf("could not encode email: %w", err)
	}
	fmt.Println("OPENING FILE")
	//Sync since we want to leave as much garanees that we can that the mail has been committed
	f, err := os.OpenFile(emailPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return err
	}
	//ulock1, err := lock(f)
	//defer ulock1()
	//if err != nil {
	//	return fmt.Errorf("could not lock email: %w", err)
	//}
	fmt.Println("COPYING EMAIL")
	_, err = io.Copy(f, emlreader)
	if err != nil {
		return errors.Join(err, f.Close(), os.Remove(emailPath))
	}
	err = f.Close()
	fmt.Println("DONW COPYING EMAIL")
	if err != nil {
		return errors.Join(err, os.Remove(emailPath))
	}

	job := Job{
		EID:     e.Metadata.Id,
		From:    e.Metadata.ReturnPath,
		Rcpt:    e.Recipients(),
		Retries: 0,
	}

	head, err := json.Marshal(job)
	if err != nil {
		err = fmt.Errorf("could not encode head: %w", err)
		return errors.Join(err, os.Remove(emailPath))

	}

	dir = eid2dir(s.cfg.Dir, queue, eid)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		err = fmt.Errorf("could not create directory: %w", err)
		return errors.Join(err, os.Remove(emailPath))
	}

	filename = fmt.Sprintf("%s.job", eid)
	jobPath := filepath.Join(dir, filename)

	f, err = os.OpenFile(jobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		err = fmt.Errorf("could not open job file: %w", err)
		return errors.Join(err, f.Close(), os.Remove(emailPath), os.Remove(jobPath))
	}
	//ulock2, err := lock(f)
	//defer ulock2()
	//if err != nil {
	//	return fmt.Errorf("could not lock email: %w", err)
	//}

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

	// TODO, inform consumer...
	return nil

}

func (s *Spool) Logf(eid xid.ID, format string, args ...interface{}) (err error) {
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
	//ulock, err := lock(f)
	//if err != nil {
	//	return fmt.Errorf("could not lock log file: %w", err)
	//}
	//defer ulock()

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("could not write log message: %w", err)
	}
	_, err = f.Write([]byte("\n"))
	return err

}

func (s *Spool) Dequeue(eid xid.ID) (*Job, io.ReadSeekCloser, error) {

	emldir := eid2dir(s.cfg.Dir, canonical, eid)
	queuedir := eid2dir(s.cfg.Dir, queue, eid)
	procdir := eid2dir(s.cfg.Dir, processing, eid)

	jobFilename := fmt.Sprintf("%s.job", eid)

	if !fileExists(filepath.Join(queuedir, jobFilename)) {
		return nil, nil, fmt.Errorf("could not find job file in queue dir, %s", filepath.Join(queuedir, jobFilename))
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
	return &head, eml, err
}

func (s *Spool) Succeed(eid xid.ID) error {

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

	return nil
}

func (s *Spool) Fail(eid xid.ID) error {

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

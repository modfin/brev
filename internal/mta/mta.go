package mta

import (
	"context"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/modfin/brev/internal/dnsx"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
	"runtime"
	"strings"
	"sync"
)

type Config struct {
}

type MTA struct {
	spool queue
	log   *logrus.Logger
	cfg   Config

	ostart sync.Once
	ostop  sync.Once

	pool *pond.WorkerPool
	dns  dnsx.MXer

	router *router
}

type queue interface {
	Queue() <-chan *spool.Job
}

func New(cfg Config, spool queue, dns dnsx.MXer, lc *tools.Logger) *MTA {

	logger := lc.New("mta")

	m := &MTA{
		spool:  spool,
		cfg:    cfg,
		log:    logger,
		dns:    dns,
		router: newRouter(),
	}

	go m.start()
	return m
}

func (m *MTA) start() {

	m.log.Infof("Starting mta, with %d-100 workers", runtime.NumCPU())
	m.pool = pond.New(100, 0, pond.MinWorkers(runtime.NumCPU()))

	for job := range m.spool.Queue() {

		if m.pool.Stopped() {
			m.log.WithField("eid", job.EID).Warn("pool stopped, skipping email")
			_ = job.Requeue()
			continue
		}

		f := m.send(job)
		m.pool.Submit(f)
		m.log.WithField("tid", job.TID).Debug("email submitted to mta")
	}
	m.pool.StopAndWait()
}

func (m *MTA) send(job *spool.Job) func() {

	return func() {

		mxs, err := m.mxsOf(job.Rcpt)
		if err != nil {
			m.log.WithError(err).WithField("tid", job.TID).Error("could not find mx servers for emails")

			_ = job.Logf("could not find mx servers for emails %s, err: %w", job.TID, err)
			_ = job.Fail()
			return
		}

		m.log.WithField("tid", job.TID).Debugf("submitting email to smtp mta router")
		err = m.router.Route(job, mxs)
		if err != nil {
			m.log.WithError(err).WithField("tid", job.TID).Error("could not route email")
			_ = job.Logf("could not route email %s, err: %w", job.TID, err)
			_ = job.Fail()
			return
		}

		m.log.WithField("tid", job.TID).Debugf("email has been sent")

		err = job.Success()
		if err != nil {
			_ = job.Logf("could not mark email %s as sent, err: %w", job.TID, err)
			m.log.WithError(err).WithField("tid", job.TID).Error("could not mark email as successful")

		}

	}
}

func (m *MTA) Stop(ctx context.Context) error {
	var err error
	m.ostop.Do(func() {

		select {
		case <-m.pool.Stop().Done():
			m.log.Info("mta has been shut down")
		case <-ctx.Done():
			err = ctx.Err()
		}

	})
	return err
}

func (m *MTA) mxsOf(emails []string) ([]string, error) {

	var err error

	emails = slicez.Map(emails, strings.ToLower)

	var domains = slicez.Reject(slicez.Map(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	}), compare.IsZero[string]())

	if len(domains) == 0 {
		return nil, errors.New("no domains found")
	}

	domains = slicez.Uniq(domains)

	if len(domains) > 1 {
		return nil, errors.New("only one domain is allowed")
	}

	domain := domains[0]

	m.log.WithField("domain", domain).Debug("mx lookup for domain")
	mxs, err := m.dns.MX(domain)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("could not find mx for domain %s, err: %w", domain, err))
	}

	if len(mxs) == 0 {
		return nil, errors.New("no mx records found")
	}

	return mxs, err
}

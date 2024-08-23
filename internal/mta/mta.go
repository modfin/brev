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
	"github.com/modfin/henry/mapz"
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
	}
	m.pool.StopAndWait()
}

func (m *MTA) send(job *spool.Job) func() {

	return func() {

		groups, err := m.groupEmails(job.Rcpt)
		if err != nil {
			_ = job.Logf("could not group all emails %s, err: %w", job.EID.String(), err)
			_ = job.Fail()
			return
		}
		if len(groups) == 0 {
			_ = job.Logf("no servers to send email to for %s", job.EID.String())
			_ = job.Fail()
			return
		}

		for _, group := range groups {
			m.router.Route(job, group)
		}

		_ = job.Success()

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

type group struct {
	Emails  []string
	Servers []string
}

func (m *MTA) groupEmails(emails []string) ([]*group, error) {
	emails = slicez.Map(emails, strings.ToLower)

	var buckets = slicez.GroupBy(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	})
	delete(buckets, "")

	var err error

	groups := mapz.Slice(buckets, func(domain string, addresses []string) *group {
		recs, lerr := m.dns.MX(domain)
		if lerr != nil {
			err = errors.Join(err, fmt.Errorf("could not find mx for domain %s, err: %w", domain, lerr))
			return nil
		}
		return &group{
			Emails:  addresses,
			Servers: recs,
		}
	})

	groups = slicez.Reject(groups, compare.IsZero[*group]())
	groups = slicez.Reject(groups, func(a *group) bool {
		return len(a.Servers) == 0 || len(a.Emails) == 0
	})
	return groups, err
}

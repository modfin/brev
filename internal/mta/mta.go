package mta

import (
	"context"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/tools"
	"github.com/rs/xid"
	"github.com/sirupsen/logrus"
	"runtime"
	"sync"
)

type Config struct {
}

type MTA struct {
	spooler spool.Spooler
	log     *logrus.Logger
	cfg     Config

	ostart sync.Once
	ostop  sync.Once

	pool *pond.WorkerPool

	mx *mx
}

func New(cfg Config, spool spool.Spooler) *MTA {

	logger := logrus.New()
	logger.AddHook(tools.LoggerWho{Name: "mta"})

	return &MTA{
		spooler: spool,
		cfg:     cfg,
		log:     logger,
		mx:      newMX(),
	}
}

func (m *MTA) Start() {

	m.ostart.Do(func() {
		go m.start()
	})

}

func (m *MTA) start() {

	m.log.Infof("Starting mta, with %d-100 workers", runtime.NumCPU())
	m.pool = pond.New(100, 0, pond.MinWorkers(runtime.NumCPU()))

	for eid := range m.spooler.Start() {
		eid := eid

		if m.pool.Stopped() {
			m.log.WithField("eid", eid).Warn("pool stopped, skipping email")
			continue
		}

		m.pool.Submit(m.send(eid))
	}
	m.pool.StopAndWait()
}

func (m *MTA) send(eid xid.ID) func() {
	return func() {
		job, in, err := m.spooler.Dequeue(eid)

		// This happens since we travers the filesystem inb the spool. This keeps the memory bound since we do not need to keep track of all emails in memory
		if errors.Is(err, spool.ErrNotFound) {
			m.log.WithField("eid", eid).Warn("email not found, skipping, (concurrent dequeue might happen with dir traversals)")
			return
		}

		if err != nil {
			m.spooler.Logf(eid, "could not dequeue email %s, err: %v", eid.String(), err)
			err = m.spooler.Fail(eid)
			return
		}
		defer in.Close()

		groups, err := m.mx.GroupEmails(job.Rcpt)
		if err != nil {
			m.spooler.Logf(eid, "could not group all emails %s, err: %v", eid.String(), err)
			m.spooler.Fail(eid)
			return
		}
		if len(groups) == 0 {
			m.spooler.Logf(eid, "no servers to send email to for %s", eid.String())
			m.spooler.Fail(eid)
			return
		}

		for i, group := range groups {

			domain, _ := tools.DomainOfEmail(group.Emails[0])
			mx := group.Servers[0]
			err = m.spooler.Logf(eid, "sending email to domain %s, through %s", domain, mx)
			if err != nil {
				m.log.WithError(err).Error("could not log email %s, err: %w", eid, err)
			}
			in := in
			if i > 0 {
				in, err = in.Clone()
				if err != nil {
					m.log.WithError(err).Error("could not clone email stream %s, err: %w", eid, err)
				}
			}

			// TODO smtp route email
			fmt.Println(domain, group.Servers)

		}

		m.spooler.Succeed(eid)

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

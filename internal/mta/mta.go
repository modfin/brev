package mta

import (
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
}

func New(cfg Config, spool spool.Spooler) *MTA {

	logger := logrus.New()
	logger.AddHook(tools.LoggerWho{Name: "mta"})

	return &MTA{
		spooler: spool,
		cfg:     cfg,
		log:     logger,
	}
}

func (m *MTA) Start() {
	m.ostart.Do(func() {
		m.start()
	})
}

func (m *MTA) start() {

	pool := pond.New(100, 0, pond.MinWorkers(runtime.NumCPU()))

	for eid := range m.spooler.Start() {
		eid := eid
		pool.Submit(m.send(eid))
	}
}

func (m *MTA) send(eid xid.ID) func() {
	return func() {
		job, in, err := m.spooler.Dequeue(eid)
		defer in.Close()
		if err != nil {
			m.spooler.Logf(eid, "could not dequeue email %s, err: %v", eid.String(), err)
			err = m.spooler.Fail(eid)
			return
		}

		groups, err := GroupEmails(job.Rcpt)
		if err != nil {
			m.spooler.Logf(eid, "could not group all emails %s, err: %v", eid.String(), err)
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

			// TODO send enqueue email group
			//https://github.com/knadh/smtppool

		}

	}
}

func (m *MTA) Stop() {
	m.ostop.Do(func() {

	})
}

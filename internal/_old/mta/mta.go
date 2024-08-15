package mta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	dao2 "github.com/modfin/brev/internal/old/dao"
	"github.com/modfin/brev/internal/old/signals"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/brev/smtpx/pool"
	"github.com/modfin/brev/tools"
	"net/textproto"
	"strings"
	"sync"
	"time"
)

// Mail Transfer Agent, sending mails to where ever they should go

func New(ctx context.Context, db dao2.DAO, dialer smtpx.Dialer, localName string) *MTA {
	done := make(chan interface{})
	m := &MTA{
		done: done,
		ctx:  ctx,
		db:   db,
		pool: pool.New(ctx, dialer, 5, localName),
		closer: func() func() {
			once := sync.Once{}
			return func() {
				once.Do(func() {
					close(done)
				})
			}
		}(),
	}
	return m
}

type MTA struct {
	done   chan interface{}
	ctx    context.Context
	db     dao2.DAO
	pool   *pool.Pool
	closer func()
}

func (m *MTA) Done() <-chan interface{} {
	return m.done
}

func (m *MTA) Stop() {
	m.closer()
}

func (m *MTA) Start(workers int) {
	fmt.Println("[MTA]: Starting MTA")
	go func() {
		err := m.start(workers)
		if err != nil {
			fmt.Println("[MTA]: got error from return", err)
		}
		m.closer()
	}()

}

func (m *MTA) start(workers int) error {

	spool := make(chan dao2.SpoolEmail, workers*2)
	localDone := make(chan interface{})
	go func() {
		select {
		case <-m.ctx.Done():
		case <-m.done:
		}

		fmt.Println("[MTA]: Shutting down mta server")
		close(localDone)
	}()

	wg := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			m.worker(spool)
			wg.Done()
		}()
	}

	newMailInSpool, cancel := signals.Listen(signals.NewMailInSpool)
	defer cancel()

	for {

		emails, err := m.db.DequeueEmails(workers * 2)

		if err != nil {
			fmt.Println(err)
		}

		if len(emails) > 0 {
			fmt.Println("[MTA]: processing", len(emails), "in internal spool")
		}

		for _, email := range emails {
			spool <- email
		}

		// if there is a queue keep working at it.
		if len(emails) > 0 {
			continue
		}

		select {
		case <-time.After(10 * time.Second):
		case <-newMailInSpool: // Wakeup signal form ingress
		case <-localDone:
			fmt.Println("[MTA]: Waiting for workers to finish")
			close(spool)
			wg.Wait()
			return errors.New("mta ordered shutdown from context")
		}

	}
}

type lg struct {
	db            dao2.DAO
	workerId      string
	transactionId int64
	messageId     string
}

func (l *lg) Logf(format string, args ...interface{}) {
	log := fmt.Sprintf(format, args...)
	log = fmt.Sprintf("[MTA-Worker %s]: %s", l.workerId, log)
	log = strings.ReplaceAll(log, "\n", "; ")
	err := l.db.AddSpecificLogEntry(l.messageId, l.transactionId, log)
	if err != nil {
		fmt.Println("could not save log entry for", l.messageId, l.transactionId)
	}
}

func (m *MTA) worker(spool chan dao2.SpoolEmail) {

	workerId := tools.RandStringRunes(5)

	fmt.Printf("[MTA-Worker %s]: Starting worker\n", workerId)
	for spoolmail := range spool {

		logger := &lg{
			db:            m.db,
			workerId:      workerId,
			transactionId: spoolmail.TransactionId,
			messageId:     spoolmail.MessageId,
		}

		logger.Logf("worker starting processing transfer")

		content, err := m.db.GetEmailContent(spoolmail.MessageId)
		if err != nil {
			logger.Logf("could not retrieve raw content of mail %s, err %v", spoolmail.MessageId, err)
			err = m.db.SetEmailStatus(spoolmail.TransactionId, dao2.BrevStatusFailed)
			if err != nil {
				logger.Logf("got error updating status, err %v", err)
			}
			continue
		}

		if len(spoolmail.MXServers) == 0 {
			logger.Logf("could not find mx server for %v", spoolmail.Recipients)
			err = m.db.SetEmailStatus(spoolmail.TransactionId, dao2.BrevStatusFailed)
			if err != nil {
				logger.Logf("got error updating status, err %v", err)
			}
			continue
		}
		// TODO iterate over mx servers i failing to connect...
		addr := spoolmail.MXServers[0]

		start := time.Now()
		err = m.pool.SendMail(logger, addr, spoolmail.From, spoolmail.Recipients, bytes.NewBuffer(content))
		stop := time.Since(start)

		if err != nil {
			terr, ok := err.(*textproto.Error)

			if !ok {
				logger.Logf("failed transfer of emails for %v, err %v", spoolmail.Recipients, err)
				err = m.db.SetEmailStatus(spoolmail.TransactionId, dao2.BrevStatusFailed)
				if err != nil {
					logger.Logf("got error updating status, err %v", err)
				}

				err = m.db.EnqueuePosthook(spoolmail,
					brev.EventFailed,
					fmt.Sprintf("failed transfer of emails for %v, err %v", spoolmail.Recipients, err))
				if err != nil {
					logger.Logf("got error adding posthook, err %v", err)
				}
				continue
			}

			leading := terr.Code / 100
			//sub := terr.Code % 100

			switch leading {
			case 4: // might be graylising
				if spoolmail.SendCount > 2 {
					logger.Logf("failing transaction after third attempt, got code %d: %s ", terr.Code, terr.Msg)
					err = m.db.SetEmailStatus(spoolmail.TransactionId, dao2.BrevStatusFailed)
					if err != nil {
						logger.Logf("got error updating status, err %v", err)
					}

					err = m.db.EnqueuePosthook(spoolmail,
						brev.EventBounce,
						fmt.Sprintf("failing delivery after third attempt, code %d: %s ", terr.Code, terr.Msg))
					if err != nil {
						logger.Logf("got error adding posthook, err %v", err)
					}

					continue
				}
				logger.Logf("rescheduling code %d: %s ", terr.Code, terr.Msg)
				err = m.db.RequeueEmail(spoolmail)
				if err != nil {
					logger.Logf("got error rescheduling email, err %v", err)
				}

				err = m.db.EnqueuePosthook(spoolmail, brev.EventDeferred, terr.Error())
				if err != nil {
					logger.Logf("got error adding posthook, err %v", err)
				}

			case 5:
				logger.Logf("failing transaction got code %d: %s ", terr.Code, terr.Msg)
				err = m.db.SetEmailStatus(spoolmail.TransactionId, dao2.BrevStatusFailed)
				if err != nil {
					logger.Logf("got error updating status, err %v", err)
				}

				err = m.db.EnqueuePosthook(spoolmail, brev.EventBounce, terr.Error())
				if err != nil {
					logger.Logf("got error adding posthook, err %v", err)
				}
			}

			continue
		}
		logger.Logf("transferred emails through %s for %v, took %v", addr, spoolmail.Recipients, stop)

		err = m.db.SetEmailStatus(spoolmail.TransactionId, dao2.BrevStatusSent)
		if err != nil {
			logger.Logf("could not update status to sent for %s, err %v\n", spoolmail.MessageId, err)
			continue
		}
		err = m.db.EnqueuePosthook(spoolmail, brev.EventDelivered, "")
		if err != nil {
			logger.Logf("got error adding posthook, err %v", err)
		}

	}
}

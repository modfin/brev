package mta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/modfin/brev/dnsx"
	"github.com/modfin/brev/internal/dao"
	"github.com/modfin/brev/internal/signals"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/brev/smtpx/pool"
	"github.com/modfin/brev/tools"
	"strings"
	"sync"
	"time"
)

// Mail Transfer Agent, sending mails to where ever they should go

func New(ctx context.Context, db dao.DAO, emailMxLookup dnsx.MXLookup, dialer smtpx.Dialer, localName string) *MTA {
	done := make(chan interface{})
	m := &MTA{
		done:          done,
		ctx:           ctx,
		db:            db,
		emailMxLookup: emailMxLookup,
		pool:          pool.New(ctx, dialer, 2, localName),
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
	done          chan interface{}
	ctx           context.Context
	db            dao.DAO
	emailMxLookup dnsx.MXLookup
	pool          *pool.Pool
	closer        func()
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

	localDone := make(chan interface{})
	go func() {
		select {
		case <-m.ctx.Done():
		case <-m.done:
		}

		fmt.Println("[MTA]: Shutting down mta server")
		close(localDone)
	}()

	spool := make(chan dao.SpoolEmail, workers*2)

	for i := 0; i < workers; i++ {
		go m.worker(spool)
	}

	newMailInSpool, cancel := signals.Listen(signals.NewMailInSpool)
	defer cancel()

	for {

		emails, err := m.db.GetQueuedEmails(workers * 2)

		if err != nil {
			fmt.Println(err)
		}

		if len(emails) > 0 {
			fmt.Println("[MTA]: processing", len(emails), "in internal spool")
		}

		for _, email := range emails {
			err := m.db.ClaimEmail(email.MessageId)
			if err != nil {
				fmt.Printf("could not claim email %s, %v\n", email.MessageId, err)
				continue
			}
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
			return errors.New("mta ordered shutdown from context")
		}

	}
}

type lg struct {
	buff []string
}

func (l *lg) Logf(format string, args ...interface{}) {
	l.buff = append(l.buff, fmt.Sprintf(format, args...))
}

func (l *lg) print() {
	fmt.Println(strings.Join(l.buff, "\n"))
	l.buff = nil
}

func (m *MTA) worker(spool chan dao.SpoolEmail) {

	logger := &lg{}

	workerId := tools.RandStringRunes(5)

	fmt.Printf("[MTA-Worker %s]: Starting worker\n", workerId)
	for spoolmail := range spool {

		content, err := m.db.GetEmailContent(spoolmail.MessageId)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not retrive raw content of mail %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}
		transferlist := m.emailMxLookup(spoolmail.Recipients)
		if len(transferlist) == 0 {
			fmt.Printf("[MTA-Worker %s] could not look up TransferList server for recipiants of mail %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}

		for _, mx := range transferlist {
			if len(mx.MXServers) == 0 {
				fmt.Printf("[MTA-Worker %s]: could not find mx server for %v\n", workerId, mx.Emails)
				continue
			}
			addr := mx.MXServers[0] + ":25"

			start := time.Now()
			err = m.pool.SendMail(logger, addr, spoolmail.From, mx.Emails, bytes.NewBuffer(content))
			stop := time.Since(start)
			logger.print()
			if err != nil {
				fmt.Printf("[MTA-Worker %s]: Faild transfer of emails to %s domain through %s for %v, err %v\n", workerId, mx.Domain, spoolmail.Recipients, stop, err)
				continue
			}
			fmt.Printf("[MTA-Worker %s]: Transferred emails to %s domain through %s for %v, took %v\n", workerId, mx.Domain, addr, mx.Emails, stop)
		}
		err = m.db.UpdateEmailBrevStatus(spoolmail.MessageId, "sent")
		if err != nil {
			fmt.Printf("[MTA-Worker %s]: could not update status to sent for %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}
	}
}

package mta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/crholm/brev/dnsx"
	"github.com/crholm/brev/internal/dao"
	"github.com/crholm/brev/internal/signals"
	"github.com/crholm/brev/smtpx"
	"github.com/crholm/brev/tools"
	"sync"
	"time"
)

// Mail Transfer Agent

func New(ctx context.Context, db dao.DAO, emailMxLookup dnsx.MXLookup, dialer smtpx.Dialer) *MTA {
	done := make(chan interface{})
	m := &MTA{
		done:          done,
		ctx:           ctx,
		db:            db,
		emailMxLookup: emailMxLookup,
		smtpDialer:    dialer,
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
	smtpDialer    smtpx.Dialer
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

func (m *MTA) worker(spool chan dao.SpoolEmail) {

	workerId := tools.RandStringRunes(5)

	fmt.Printf("[MTA-Worker %s]: Starting worker\n", workerId)
	for spoolmail := range spool {

		content, err := m.db.GetEmailContent(spoolmail.MessageId)

		transferlist := m.emailMxLookup(spoolmail.Recipients)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not look up TransferList server for recipiants of mail %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}

		fmt.Printf("\nTransferlist%+v\n", transferlist)

		for _, mx := range transferlist {
			if len(mx.MXServers) == 0 {
				fmt.Printf("[MTA-Worker %s]: could not find mx server for %v\n", workerId, mx.Emails)
				continue
			}
			addr := mx.MXServers[0] + ":25"

			start := time.Now()
			fmt.Printf("[MTA-Worker %s]: opening connection to %v, ", workerId, addr)
			conn, err := m.smtpDialer(addr, nil)
			fmt.Printf("took %v\n", time.Now().Sub(start))
			if err != nil {
				fmt.Printf("[MTA-Worker %s]: could not establis connection to mx server %s for %v, err %v\n", workerId, addr, mx.Emails, err)
				continue
			}

			for _, mailaddr := range mx.Emails {
				start = time.Now()
				fmt.Printf("[MTA-Worker %s]: Transferring emails to %s domain through %s for %s, ", workerId, mx.Domain, addr, mailaddr)
				err = conn.SendMail(spoolmail.From, []string{mailaddr}, bytes.NewBuffer(content))
				fmt.Printf("took %v\n", time.Now().Sub(start))
				if err != nil {
					fmt.Printf("[MTA-Worker %s]: could not transfer mail to %s for mail %s to %s, err %v\n", workerId, addr, spoolmail.MessageId, mailaddr, err)
					continue
				}
			}
			fmt.Printf("[MTA-Worker %s]: Transfer compleat for mail %s\n", workerId, spoolmail.MessageId)
			start = time.Now()
			fmt.Printf("[MTA-Worker %s]: cloasing connection to %v", workerId, addr)
			err = conn.Close()
			fmt.Printf("took %v\n", time.Now().Sub(start))
			if err != nil {
				fmt.Printf("[MTA-Worker %s]: failing to cloase connetion to %s, err %v\n", workerId, addr, err)
				continue
			}
		}

	}
}

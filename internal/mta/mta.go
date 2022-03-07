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
	"net/textproto"
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

				terr, ok := err.(*textproto.Error)
				if !ok {
					fmt.Printf("[MTA-Worker %s]: Faild transfer of emails to %s domain through %s for %v, err %v\n", workerId, mx.Domain, spoolmail.Recipients, time.Since(start), err)
					continue
				}

				switch terr.Code {
				case 421: // Service not available, closing transmission channel (This may be a reply to any command if the service knows it must shut down)
				case 432: // 4.7.12 A password transition is needed [3]
				case 450: // Requested mail action not taken: mailbox unavailable (e.g., mailbox busy or temporarily blocked for policy reasons)
				case 451: // Requested action aborted: local error in processing, 4.4.1 IMAP server unavailable [4] // Anti spam
				case 452: // Requested action not taken: insufficient system storage
				case 454: // 4.7.0 Temporary authentication failure [3]
				case 455: // Server unable to accommodate parameters
				case 471: // An error of your mail server, often due to an issue of the local anti-spam filter.
				case 500: // Syntax error, command unrecognized (This may include errors such as command line too long),  5.5.6 Authentication Exchange line is too long [3]
				case 501: // Syntax error in parameters or arguments, 5.5.2 Cannot Base64-decode Client responses [3], 5.7.0 Client initiated Authentication Exchange (only when the SASL mechanism specified that client does not begin the authentication exchange) [3]
				case 502: // Command not implemented
				case 503: // Bad sequence of commands
				case 504: // Command parameter is not implemented,  5.5.4 Unrecognized authentication type [3]
				case 521: // Server does not accept mail [5]
				case 523: // Encryption Needed [6]
				case 530: // 5.7.0 Authentication required [3]
				case 534: // 5.7.9 Authentication mechanism is too weak [3]
				case 535: // 5.7.8 Authentication credentials invalid [3]
				case 538: // 5.7.11 Encryption required for requested authentication mechanism[3]
				case 541: // The recipient address rejected your message: normally, it’s an error caused by an anti-spam filter.
				case 550: // Requested action not taken: mailbox unavailable (e.g., mailbox not found, no access, or command rejected for policy reasons)
				case 551: // User not local; please try <forward-path> // Anti spam code...
				case 552: // Requested mail action aborted: exceeded storage allocation
				case 553: // Requested action not taken: mailbox name not allowed
				case 554: // Transaction has failed (Or, in the case of a connection-opening response, "No SMTP service here"), 5.3.4 Message too big for system [4]
				case 556: // Domain does not accept mail [5]
				}

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

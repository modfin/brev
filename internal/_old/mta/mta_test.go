package mta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/internal/old/dao"
	"github.com/modfin/brev/internal/old/signals"
	"github.com/modfin/brev/smtpx"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type (
	testConnection struct {
		mu            sync.Mutex
		t             *testing.T
		client        *smtpx.Client
		sendErr       error
		sendDelay     time.Duration
		gotSentFrom   string
		gotSentTo     [][]string
		sentEmailChan chan<- sentEmail
	}
	dbMail struct {
		email   *dao.SpoolEmail
		content []byte
	}
	testDAO struct {
		mu                      sync.Mutex
		t                       *testing.T
		lastTxID                int64
		queuedMails             map[int64]*dbMail
		mailContent             map[string][]byte
		getEmailContentErr      error
		SetEmailStatusErr       error
		gotEnqueuePosthookEvent brev.PosthookEvent
		gotGetMessageId         string
		gotUpdateTransactionId  int64
		gotUpdateStatus         string
		log                     []string
	}
	sentEmail struct {
		from string
		to   []string
		body string
	}
)

var testErr = errors.New("test error")

func TestMTA_worker(t *testing.T) {
	type testCase struct {
		name                     string
		messageId                string
		recipients               []string
		dialerErr                error
		getEmailContentErr       error
		setEmailStatusErr        error
		sendErr                  error
		sendDelay                time.Duration
		wantDialed               bool
		wantGetMessageId         string
		wantSentFrom             string
		wantSentTo               [][]string
		wantStatus               string
		wantEnqueuePosthookEvent brev.PosthookEvent
	}
	for _, tc := range []testCase{
		{
			name:                     "happy flow",
			messageId:                "test0",
			recipients:               []string{"test1@localhost", "test2@127.0.0.1", "test3@localhost", "test4@127.0.0.1"},
			wantGetMessageId:         "test0",
			wantDialed:               true,
			wantSentFrom:             "test@localhost",
			wantSentTo:               [][]string{{"test1@localhost", "test3@localhost"}, {"test2@127.0.0.1", "test4@127.0.0.1"}},
			wantStatus:               dao.BrevStatusSent,
			wantEnqueuePosthookEvent: brev.EventDelivered,
		},
		{
			name:               "GetEmailContent error",
			messageId:          "test1",
			recipients:         []string{"test1@localhost"},
			getEmailContentErr: testErr,
			wantGetMessageId:   "test1",
			wantStatus:         dao.BrevStatusFailed,
			//wantEnqueuePosthookEvent: brev.EventFailed, TODO?
		},
		{
			name:               "GetEmailContent/SetEmailStatus error",
			messageId:          "test1.1",
			recipients:         []string{"test1@localhost"},
			getEmailContentErr: testErr,
			setEmailStatusErr:  testErr,
			wantGetMessageId:   "test1.1",
			wantStatus:         dao.BrevStatusFailed,
			//wantEnqueuePosthookEvent: brev.EventFailed, TODO?
		},
		{
			name:             "empty lookup result",
			messageId:        "test2",
			recipients:       []string{"test2@"},
			wantGetMessageId: "test2",
			wantStatus:       dao.BrevStatusFailed,
			//wantEnqueuePosthookEvent: brev.EventFailed, TODO?
		},
		{
			name:              "empty lookup result/SetEmailStatus error",
			messageId:         "test2.1",
			recipients:        []string{"test2@"},
			setEmailStatusErr: testErr,
			wantGetMessageId:  "test2.1",
			wantStatus:        dao.BrevStatusFailed,
			//wantEnqueuePosthookEvent: brev.EventFailed, TODO?
		},
		{
			name:                     "send error",
			messageId:                "test4",
			recipients:               []string{"test4@localhost"},
			sendErr:                  testErr,
			wantGetMessageId:         "test4",
			wantDialed:               true,
			wantSentFrom:             "test@localhost",
			wantSentTo:               [][]string{{"test4@localhost"}},
			wantStatus:               dao.BrevStatusFailed,
			wantEnqueuePosthookEvent: brev.EventFailed,
		},
		{
			name:                     "send error/SetEmailStatus error",
			messageId:                "test4.1",
			recipients:               []string{"test4.1@localhost"},
			sendErr:                  testErr,
			setEmailStatusErr:        testErr,
			wantGetMessageId:         "test4.1",
			wantDialed:               true,
			wantSentFrom:             "test@localhost",
			wantSentTo:               [][]string{{"test4.1@localhost"}},
			wantStatus:               dao.BrevStatusFailed,
			wantEnqueuePosthookEvent: brev.EventFailed,
		},
		// TODO: Add send error tests that return a textproto.Error with 4xx and 5xx codes
		{
			name:              "SetEmailStatus error after send",
			messageId:         "test5",
			recipients:        []string{"test5@localhost"},
			setEmailStatusErr: testErr,
			wantGetMessageId:  "test5",
			wantDialed:        true,
			wantSentFrom:      "test@localhost",
			wantSentTo:        [][]string{{"test5@localhost"}},
			wantStatus:        dao.BrevStatusSent,
			//wantEnqueuePosthookEvent: brev.EventDelivered, TODO?
		},
	} {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				//t.Parallel()
				fmt.Printf("\n[Test]: starting %s\n", tc.name)
				var gotDialed bool
				conn := &testConnection{t: t, sendErr: tc.sendErr, sendDelay: tc.sendDelay}
				testDao := &testDAO{t: t, getEmailContentErr: tc.getEmailContentErr, SetEmailStatusErr: tc.setEmailStatusErr}
				testDialer := func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
					t.Logf("Dailer('%s') (%v)", addr, tc.dialerErr)
					gotDialed = true
					if tc.dialerErr != nil {
						return nil, tc.dialerErr
					}
					return conn, nil
				}

				mta := New(context.Background(), testDao, testDialer, "test")

				mails, _ := spoolMails(tc.messageId, "test@localhost", tc.recipients)
				t.Logf("Adding %d mails to spool channel", len(mails))
				spool := make(chan dao.SpoolEmail, len(mails))
				for _, m := range mails {
					spool <- m
				}
				close(spool)
				mta.worker(spool)

				// DAO
				if testDao.gotGetMessageId != tc.wantGetMessageId {
					t.Errorf("ERROR: got dao.GetEmailContent messageID: %s, want: %s", testDao.gotGetMessageId, tc.wantGetMessageId)
				}
				//if testDao.gotUpdateTransactionId != tc.wantUpdateMessageId {
				//	t.Errorf("ERROR: got dao.SetEmailStatus messageID: %s, want: %s", testDao.gotUpdateMessageId, tc.wantUpdateMessageId)
				//}
				if testDao.gotUpdateStatus != tc.wantStatus {
					t.Errorf("ERROR: got dao.SetEmailStatus status: %s, want: %s", testDao.gotUpdateStatus, tc.wantStatus)
				}
				if testDao.gotEnqueuePosthookEvent != tc.wantEnqueuePosthookEvent {
					t.Errorf("ERROR: got dao.EnqueuePosthook event: %s, want: %s", testDao.gotEnqueuePosthookEvent, tc.wantEnqueuePosthookEvent)
				}

				// Dialer
				if gotDialed != tc.wantDialed {
					t.Errorf("ERROR: got dialed: %v, want: %v", gotDialed, tc.wantDialed)
				}

				// Connection
				if conn.gotSentFrom != tc.wantSentFrom {
					t.Errorf("ERROR: got from address: %s, want: %s", conn.gotSentFrom, tc.wantSentFrom)
				}
				if len(conn.gotSentTo) != len(tc.wantSentTo) {
					t.Fatalf("ERROR: got sent TO: %v, want: %v", conn.gotSentTo, tc.wantSentTo)
				}
				for _, want := range tc.wantSentTo {
					var found bool
					for _, got := range conn.gotSentTo {
						found = reflect.DeepEqual(got, want)
						if found {
							break
						}
					}
					if !found {
						t.Errorf("ERROR: did not get: %v", want)
					}
				}
			}
		}(tc))
	}
}

func TestMTA(t *testing.T) {
	type testCase struct {
		name            string
		emailRecipients [][]string
		wantedMails     int
	}

	for i, tc := range []testCase{
		{
			name:            "<one mail, same domain recipients>",
			emailRecipients: [][]string{{"test1-1@test.local", "test1-2@test.local", "test1-3@test.local", "test1-4@test.local"}},
			wantedMails:     1,
		},
		{
			name: "<two mails, same domain recipients>",
			emailRecipients: [][]string{
				{"test2-1-1@test.local", "test2-1-2@test.local", "test2-1-3@test.local", "test2-1-4@test.local"},
				{"test2-2-1@test.local", "test2-2-2@test.local", "test2-2-3@test.local", "test2-2-4@test.local"},
			},
			wantedMails: 2,
		},
		{
			name: "<two mails, unique domain recipients>",
			emailRecipients: [][]string{
				{"test2-1-1@test1.local", "test2-1-2@test2.local", "test2-1-3@test3.local", "test2-1-4@test4.local"},
				{"test2-2-1@test5.local", "test2-2-2@test6.local", "test2-2-3@test7.local", "test2-2-4@test8.local"},
			},
			wantedMails: 8,
		},
	} {
		t.Run(tc.name, func(i int, tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				mailChan := make(chan sentEmail)
				defer close(mailChan)
				conn := &testConnection{t: t, sentEmailChan: mailChan}
				dialer := func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
					return conn, nil
				}

				// Setup test context
				testCtx, cancel := context.WithCancel(context.Background())
				defer cancel()

				testDao := &testDAO{t: t}

				// Start MTA
				mta := New(testCtx, testDao, dialer, "localhost")
				t.Logf("Starting MTA workers..")
				mta.Start(1)
				defer mta.Stop()

				// Queue mail
				t.Logf("Adding mail to spool..")
				for i, rcpt := range tc.emailRecipients {
					_, err := testDao.EnqueueEmails(spoolMails(fmt.Sprintf("%s-mail#%d", tc.name, i), tc.name, rcpt))
					if err != nil {
						t.Fatalf("EnqueueEmails returned: %v", err)
					}
				}

				// Informs MTA to wake up and start processing mail if a sleep at the moment.
				started := time.Now()
				t.Logf("Sending signal..")
				signals.Broadcast(signals.NewMailInSpool)

				loop := true
				var mails []sentEmail
				for loop {
					select {
					case m := <-mailChan:
						t.Logf("GOT: %#v", m)
						mails = append(mails, m)
						//if len(mails) == tc.wantedMails {
						//	// Got wanted number of emails now.
						//	loop = false
						//}
					case <-time.After(100 * time.Millisecond):
						// Did not get any (more) mail for 100ms, abort.
						loop = false
					}
				}
				elapsedTime := time.Since(started)
				t.Logf("Got %d mails in %v", len(mails), elapsedTime)
				if len(mails) != tc.wantedMails {
					t.Errorf("Got %d mails, want %d mails", len(mails), tc.wantedMails)
					for _, s := range mails {
						t.Logf("FROM: %s, TO: %s", s.from, s.to)
					}
				}
			}
		}(i, tc))
	}
}

/*
	func testLookup(lookupErr error) func(emails []string) []dnsx.TransferList {
		return func(emails []string) []dnsx.TransferList {
			if lookupErr != nil {
				return []dnsx.TransferList{{Err: lookupErr}}
			}
			if len(emails) == 0 {
				return nil
			}
			var mx []dnsx.TransferList
			var buckets = map[string][]string{}
			for _, address := range emails {
				domain, err := tools.DomainOfEmail(address)
				if err != nil {
					continue
				}
				buckets[domain] = append(buckets[domain], address)
			}
			for domain, addresses := range buckets {
				mx = append(mx, dnsx.TransferList{Domain: domain, Emails: addresses, MXServers: []string{"127.0.0.1"}})
			}
			return mx
		}
	}
*/
func spoolMails(id, from string, recipients []string) ([]dao.SpoolEmail, []byte) {
	var transactionId int64

	var mails []dao.SpoolEmail

	domainGroups := map[string][]string{}

	for _, recipient := range recipients {
		domain := strings.Split(recipient, "@")[1]
		domainGroups[domain] = append(domainGroups[domain], recipient)
	}
	for domain, addrs := range domainGroups {
		var mxServers []string
		transactionId = rand.Int63()
		if domain != "" {
			mxServers = []string{fmt.Sprintf("mx1.%s:25", domain)}
		}
		mails = append(mails, dao.SpoolEmail{
			TransactionId: transactionId,
			MessageId:     id,
			From:          from,
			Recipients:    addrs,
			MXServers:     mxServers,
		})
	}

	return mails, []byte(fmt.Sprintf("Subject: %s\r\ntest mail", id))
}

func (tc *testConnection) SendMail(from string, to []string, msg io.WriterTo) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.t != nil {
		tc.t.Logf("conn.SendMail(%s, %v)", from, to)
	}

	tc.gotSentFrom = from
	tc.gotSentTo = append(tc.gotSentTo, to)

	if tc.sendDelay > 0 {
		time.Sleep(tc.sendDelay)
	}

	if tc.sentEmailChan != nil {
		// Send the e-mail to a channel that the test is reading from...
		body := &bytes.Buffer{}
		msg.WriteTo(body)
		tc.sentEmailChan <- sentEmail{from: from, to: to, body: body.String()}
	}

	return tc.sendErr
}

func (tc *testConnection) SetLogger(logger smtpx.Logger) {
	if logger != nil {
		logger.Logf("test")
	}
}

func (tc *testConnection) Close() error {
	if tc.t != nil {
		tc.t.Log("conn.Close()")
	}
	return nil
}

func (td *testDAO) EnsureApiKey(s string) error {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) UpsertApiKey(key dao.ApiKey) (dao.ApiKey, error) {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) EnqueueEmails(emails []dao.SpoolEmail, content []byte) (transactionIds []int64, err error) {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.t != nil {
		td.t.Log("dao.EnqueueEmails(...)")
	}

	if td.queuedMails == nil {
		td.queuedMails = make(map[int64]*dbMail)
	}

	if td.mailContent == nil {
		td.mailContent = map[string][]byte{}
	}

	for _, e := range emails {
		email := e
		email.Status = dao.BrevStatusQueued
		td.mailContent[email.MessageId] = content
		td.queuedMails[email.TransactionId] = &dbMail{email: &email, content: content}
		td.lastTxID++
		transactionIds = append(transactionIds, td.lastTxID)
	}

	if td.t != nil {
		td.t.Logf("EnqueueEmails queue: %#v", td.queuedMails)
	}
	return transactionIds, nil
}

func (td *testDAO) DequeueEmails(limit int) (emails []dao.SpoolEmail, err error) {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.t != nil {
		td.t.Logf("dao.DequeueEmails(%d)", limit)
	}

	if td.queuedMails == nil {
		return nil, nil
	}
	now := time.Now().UTC()
	for _, m := range td.queuedMails {
		if m.email.Status == dao.BrevStatusQueued && m.email.SendAt.Before(now) && m.email.SendCount <= 3 {
			m.email.SendCount++
			m.email.UpdatedAt = now
			m.email.Status = dao.BrevStatusProcessing
			emails = append(emails, *m.email)
			if len(emails) == limit {
				break
			}
		}
	}
	return emails, nil
}

func (td *testDAO) RequeueEmail(emails dao.SpoolEmail) error {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) SetEmailStatus(transactionId int64, statusBrev string) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.t != nil {
		td.t.Logf("dao.SetEmailStatus(%d, '%s') (%v)", transactionId, statusBrev, td.SetEmailStatusErr)
	}

	td.gotUpdateTransactionId = transactionId
	td.gotUpdateStatus = statusBrev

	if td.queuedMails != nil {
		td.queuedMails[transactionId].email.Status = statusBrev
	}

	return td.SetEmailStatusErr
}

func (td *testDAO) GetEmail(transactionId int64) (dao.SpoolEmail, error) {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) AddSpecificLogEntry(messageId string, transactionId int64, log string) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.t != nil {
		td.t.Logf("dao.AddSpecificLogEntry('%s', %d, '%s')", messageId, transactionId, log)
	}

	td.log = append(td.log, fmt.Sprintf("[%s %d] %s", messageId, transactionId, log))
	return nil
}

func (td *testDAO) AddLogEntry(messageId string, log string) error {
	if td.t != nil {
		td.t.Logf("dao.AddLogEntry('%s', '%s')", messageId, log)
	}
	return nil
}

func (td *testDAO) EnqueuePosthook(email dao.SpoolEmail, event brev.PosthookEvent, message string) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	td.gotEnqueuePosthookEvent = event

	if td.t != nil {
		td.t.Logf("dao.EnqueuePosthook(%v, '%s', '%s')", email.Recipients, event, message)
	}
	return nil
}

func (td *testDAO) DequeuePosthook(count int) ([]dao.Posthook, error) {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) GetApiKey(key string) (dao.ApiKey, error) { return dao.ApiKey{}, nil }

func (td *testDAO) GetEmailContent(messageId string) ([]byte, error) {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.t != nil {
		td.t.Logf("dao.GetEmailContent('%s') ([]byte, %v)", messageId, td.getEmailContentErr)
	}
	td.gotGetMessageId = messageId
	if td.getEmailContentErr != nil {
		return nil, td.getEmailContentErr
	}

	if td.queuedMails == nil {
		return []byte("test content"), nil
	}

	content := td.mailContent[messageId]
	return content, nil
}

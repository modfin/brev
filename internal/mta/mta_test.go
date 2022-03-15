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
	"github.com/modfin/brev/tools"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type (
	testConnection struct {
		mu            sync.Mutex
		client        *smtpx.Client
		sendErr       error
		sendDelay     time.Duration
		gotSentFrom   string
		gotSentTo     [][]string
		sentEmailChan chan<- sentEmail
	}
	dbMail struct {
		status  string
		email   dao.SpoolEmail
		content []byte
	}
	testDAO struct {
		mu                       sync.Mutex
		queuedMails              map[int64]*dbMail
		mailContent              map[string][]byte
		getEmailContentErr       error
		updateEmailBrevStatusErr error
		gotGetMessageId          string
		gotUpdateTransactionId   int64
		gotUpdateStatus          string
		log                      []string
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
		name             string
		messageId        string
		recipients       []string
		dialerErr        error
		emailContentErr  error
		lookupErr        error
		sendErr          error
		updateErr        error
		sendDelay        time.Duration
		wantDialed       bool
		wantGetMessageId string
		//wantUpdateMessageId string
		wantSentFrom string
		wantSentTo   [][]string
		wantStatus   string
	}
	for _, tc := range []testCase{
		{
			name:             "happy flow",
			messageId:        "test0",
			recipients:       []string{"test1@localhost", "test2@127.0.0.1", "test3@localhost", "test4@127.0.0.1"},
			wantGetMessageId: "test0",
			//wantUpdateMessageId: "test0",
			wantDialed:   true,
			wantSentFrom: "test@localhost",
			wantSentTo:   [][]string{{"test1@localhost", "test3@localhost"}, {"test2@127.0.0.1", "test4@127.0.0.1"}},
			wantStatus:   "sent",
		},
		{
			name:             "GetEmailContent error",
			messageId:        "test1",
			emailContentErr:  testErr,
			wantGetMessageId: "test1",
		},
		{
			name:             "empty lookup result",
			messageId:        "test2",
			wantGetMessageId: "test2",
		},
		{
			name:             "lookup error",
			messageId:        "test3",
			lookupErr:        testErr,
			wantGetMessageId: "test3",
			//wantUpdateMessageId: "test3",
			wantDialed: false,
			wantStatus: "sent",
		},
		{
			name:             "send error",
			messageId:        "test4",
			recipients:       []string{"test@localhost"},
			sendErr:          testErr,
			wantGetMessageId: "test4",
			//wantUpdateMessageId: "test4",
			wantDialed:   true,
			wantSentFrom: "test@localhost",
			wantSentTo:   [][]string{{"test@localhost"}},
			wantStatus:   "sent",
		},
		{
			name:             "update error",
			messageId:        "test4",
			recipients:       []string{"test@localhost"},
			updateErr:        testErr,
			wantGetMessageId: "test4",
			//wantUpdateMessageId: "test4",
			wantDialed:   true,
			wantSentFrom: "test@localhost",
			wantSentTo:   [][]string{{"test@localhost"}},
			wantStatus:   "sent",
		},
	} {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				var gotDialed bool
				conn := &testConnection{sendErr: tc.sendErr, sendDelay: tc.sendDelay}
				testDao := &testDAO{getEmailContentErr: tc.emailContentErr, updateEmailBrevStatusErr: tc.updateErr}
				testDialer := func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
					gotDialed = true
					if tc.dialerErr != nil {
						return nil, tc.dialerErr
					}
					return conn, nil
				}

				mta := New(context.Background(), testDao, testLookup(tc.lookupErr), testDialer, "test")

				spool := make(chan dao.SpoolEmail, 1)
				spool <- dao.SpoolEmail{
					MessageId:  tc.messageId,
					From:       "test@localhost",
					Recipients: tc.recipients,
				}
				close(spool)
				mta.worker(spool)

				// DAO
				if testDao.gotGetMessageId != tc.wantGetMessageId {
					t.Errorf("ERROR: got dao.GetEmailContent messageID: %s, want: %s", testDao.gotGetMessageId, tc.wantGetMessageId)
				}
				//if testDao.gotUpdateTransactionId != tc.wantUpdateMessageId {
				//	t.Errorf("ERROR: got dao.UpdateEmailBrevStatus messageID: %s, want: %s", testDao.gotUpdateMessageId, tc.wantUpdateMessageId)
				//}
				if testDao.gotUpdateStatus != tc.wantStatus {
					t.Errorf("ERROR: got dao.UpdateEmailBrevStatus status: %s, want: %s", testDao.gotUpdateStatus, tc.wantStatus)
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
			name:            "<two mails, same domain recipients>",
			emailRecipients: [][]string{{"test2-1-1@test.local", "test2-1-2@test.local", "test2-1-3@test.local", "test2-1-4@test.local"}, {"test2-2-1@test.local", "test2-2-2@test.local", "test2-2-3@test.local", "test2-2-4@test.local"}},
			wantedMails:     2,
		},
		{
			name:            "<two mails, unique domain recipients>",
			emailRecipients: [][]string{{"test2-1-1@test1.local", "test2-1-2@test2.local", "test2-1-3@test3.local", "test2-1-4@test4.local"}, {"test2-2-1@test5.local", "test2-2-2@test6.local", "test2-2-3@test7.local", "test2-2-4@test8.local"}},
			wantedMails:     8,
		},
	} {
		t.Run(tc.name, func(i int, tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				mailChan := make(chan sentEmail)
				defer close(mailChan)
				conn := &testConnection{sentEmailChan: mailChan}
				dialer := func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
					return conn, nil
				}

				// Setup test context
				testCtx, cancel := context.WithCancel(context.Background())
				defer cancel()

				testDao := &testDAO{}

				// Start MTA
				mta := New(testCtx, testDao, testLookup(nil), dialer, "localhost")
				t.Logf("Starting MTA workers..")
				mta.Start(1)
				defer mta.Stop()

				// Queue mail
				t.Logf("Adding mail to spool..")
				for i, rcpt := range tc.emailRecipients {
					testDao.AddEmailToSpool(spoolMails(fmt.Sprintf("%s-mail#%d", tc.name, i), tc.name, rcpt))
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
						if len(mails) == tc.wantedMails {
							// Got wanted number of emails now.
							loop = false
						}
					case <-time.After(250 * time.Millisecond):
						// Did not get any (more) mail for 250ms, abort.
						loop = false
					}
				}
				elapsedTime := time.Since(started)
				t.Logf("Got %d mails in %v", len(mails), elapsedTime)
				if len(mails) != tc.wantedMails {
					t.Errorf("Got %d mails, want %d mails", len(mails), tc.wantedMails)
				}
			}
		}(i, tc))
	}
}

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

func spoolMails(id, from string, recipients []string) ([]dao.SpoolEmail, []byte) {
	var transactionId int64

	var mails []dao.SpoolEmail

	domainGroups := map[string][]string{}

	for _, recipient := range recipients {
		domain := strings.Split(recipient, "@")[1]
		domainGroups[domain] = append(domainGroups[domain], recipient)
	}

	for _, addrs := range domainGroups {
		transactionId += 1
		mails = append(mails, dao.SpoolEmail{
			TransactionId: transactionId,
			MessageId:     id,
			From:          from,
			Recipients:    addrs,
			SendAt:        time.Now().UTC(),
		})
	}

	return mails, []byte(fmt.Sprintf("Subject: %s\r\ntest mail", id))
}

func (tc *testConnection) SendMail(from string, to []string, msg io.WriterTo) error {
	tc.gotSentFrom = from
	tc.mu.Lock()
	tc.gotSentTo = append(tc.gotSentTo, to)
	tc.mu.Unlock()
	if tc.sendDelay > 0 {
		time.Sleep(tc.sendDelay)
	}
	if tc.sentEmailChan != nil {
		body := &bytes.Buffer{}
		msg.WriteTo(body)
		tc.sentEmailChan <- sentEmail{from: from, to: to, body: body.String()}
	}
	return tc.sendErr
}

func (tc *testConnection) SetLogger(logger smtpx.Logger) {
	logger.Logf("test")
}

func (tc *testConnection) Close() error {
	return nil
}

func (td *testDAO) GetApiKey(key string) (*dao.ApiKey, error) { return nil, nil }

func (td *testDAO) ClaimEmail(transactionId int64) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.queuedMails == nil {
		return nil
	}

	m := td.queuedMails[transactionId]
	if m.status != "queued" {
		return fmt.Errorf("could not claim email %d, %d was affected by claim atempt", transactionId, 0)
	}
	m.status = "processing"
	return nil
}

func (td *testDAO) UpdateSendCount(email dao.SpoolEmail) error {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) AddEmailToSpool(spoolmails []dao.SpoolEmail, content []byte) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.queuedMails == nil {
		td.queuedMails = make(map[int64]*dbMail)
	}

	if td.mailContent == nil {
		td.mailContent = map[string][]byte{}
	}

	for _, email := range spoolmails {
		td.mailContent[email.MessageId] = content
		td.queuedMails[email.TransactionId] = &dbMail{status: "queued", email: email, content: content}
	}

	return nil
}

func (td *testDAO) RescheduleEmailToSpool(spoolmail dao.SpoolEmail) error {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) AddSpoolSpecificLogEntry(messageId string, transactionId int64, log string) error {
	//TODO implement me
	td.log = append(td.log, fmt.Sprintf("[%s %d] %s", messageId, transactionId, log))
	return nil
}

func (td *testDAO) AddSpoolLogEntry(messageId string, log string) error {
	//TODO implement me
	panic("implement me")
}

func (td *testDAO) GetEmailContent(messageId string) ([]byte, error) {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.queuedMails == nil {
		td.gotGetMessageId = messageId
		if td.getEmailContentErr != nil {
			return nil, td.getEmailContentErr
		}
		return []byte("test content"), nil
	}

	content := td.mailContent[messageId]
	return content, nil
}

func (td *testDAO) GetQueuedEmails(count int) (emails []dao.SpoolEmail, err error) {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.queuedMails == nil {
		return nil, nil
	}

	for _, m := range td.queuedMails {
		if m.status == "queued" {
			emails = append(emails, m.email)
			if len(emails) == count {
				break
			}
		}
	}
	return emails, nil
}

func (td *testDAO) UpdateEmailBrevStatus(transactionId int64, statusBrev string) error {
	td.mu.Lock()
	defer td.mu.Unlock()

	if td.queuedMails == nil {
		td.gotUpdateTransactionId = transactionId
		td.gotUpdateStatus = statusBrev
		return td.updateEmailBrevStatusErr
	}

	td.queuedMails[transactionId].status = statusBrev
	return nil
}

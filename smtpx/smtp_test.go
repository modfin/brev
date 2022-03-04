package smtpx

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/quotedprintable"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type (
	smtpServer struct {
		stopFunc   context.CancelFunc
		ListenAddr string
		socket     net.Listener
		mailChan   chan *Email
		mailQueue  []*Email
		waitList   map[chan *Email]filter
		mu         sync.Mutex
		wg         sync.WaitGroup
	}
	filter struct {
		testName      string
		wantedFrom    string
		wantedTo      string
		wantedSubject string
	}
	Email struct {
		From    string
		To      string
		Subject string
		Body    string
	}
)

const (
	smtpLog        = false
	smtpLogVerbose = false
)

func init() {
	testHookStartTLS = func(config *tls.Config) {
		config.InsecureSkipVerify = true
	}
}

func TestSendMail(t *testing.T) {
	svr := newTestServer(t)
	defer svr.Stop(t)

	// Setup test context
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	from := "testsendmail@localhost.local"
	to := "test@example.com"
	content := bytes.NewBufferString("Subject:TestSendMail\r\nBody..")

	// Setup trigger and channel for receiving sent test email(s)
	sentMailChan := svr.AwaitEmail(testCtx, "TestSendMail", from, to, "TestSendMail")

	err := SendMail(t, svr.ListenAddr, "localhost", nil, from, []string{to}, content)
	if err != nil {
		t.Fatalf("smtpx.SendMail() returned error: %v", err)
	}

	var got *Email
	select {
	case got = <-sentMailChan:
	case <-time.After(250 * time.Millisecond):
	}
	if got == nil {
		t.Fatalf("ERROR: did not get any email")
	}
	if got.From != "<"+from+">" {
		t.Errorf("ERROR: got from: %s, want: <%s>", got.From, from)
	}
	if got.To != "<"+to+">" {
		t.Errorf("ERROR: got to: %s, want: <%s>", got.To, to)
	}
}

func TestConnection(t *testing.T) {
	svr := newTestServer(t)
	defer svr.Stop(t)

	from := "TestConnection@localhost.local"
	to := "test@example.com"
	content := bytes.NewBufferString("Subject:TestConnection\r\nBody..")

	// Setup test context
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup trigger and channel for receiving sent test email(s)
	sentMailChan := svr.AwaitEmail(testCtx, "TestConnection", from, to, "TestConnection")

	// Create a connection and send the email
	conn, err := NewConnection(t, svr.ListenAddr, "localhost", nil)
	if err != nil {
		t.Fatalf("ERROR: smtpx.NewConnection returned error: %v", err)
	}

	conn.SendMail(from, []string{to}, content)

	var got *Email
	select {
	case got = <-sentMailChan:
	case <-time.After(250 * time.Millisecond):
	}
	if got == nil {
		t.Fatalf("ERROR: did not get any email")
	}
	if got.From != "<"+from+">" {
		t.Errorf("ERROR: got from: %s, want: <%s>", got.From, from)
	}
	if got.To != "<"+to+">" {
		t.Errorf("ERROR: got to: %s, want: <%s>", got.To, to)
	}
}

func newTestServer(t *testing.T) *smtpServer {
	t.Log("SMTP: Staring SMTP test server...")

	keypair, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		t.Fatalf("SMTP: failed to load certificate: %v", err)
		return nil
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{keypair}, MinVersion: tls.VersionTLS12}

	ln, err := net.Listen("tcp4", "")
	if err != nil {
		t.Errorf("SMTP: failed to create listening socket: %v", err)
		return nil
	}

	ctx, stopFunc := context.WithCancel(context.Background())

	svr := &smtpServer{
		ListenAddr: ln.Addr().String(),
		mailChan:   make(chan *Email),
		socket:     ln,
		stopFunc:   stopFunc,
	}
	go smtpListen(ctx, &svr.wg, ln, tlsConfig, svr.mailChan)
	go func() {
		svr.wg.Add(1)
		defer svr.wg.Done()

		for email := range svr.mailChan {
			svr.mu.Lock()
			svr.mailQueue = append(svr.mailQueue, email)
			i := len(svr.mailQueue)
			for chn, f := range svr.waitList {
				if f.wantedFrom != "" && !strings.Contains(email.From, f.wantedFrom) {
					continue
				}
				if f.wantedTo != "" && !strings.Contains(email.To, f.wantedTo) {
					continue
				}
				if f.wantedSubject != "" && !strings.Contains(email.Subject, f.wantedSubject) {
					continue
				}

				// Sending email to awaiting channels will probably block from time to time, even with buffered channels.
				// So we should make sure that tests that haven't yet started to listen to the channel, can't stop waiting
				// on a lock on s.mu, causing a deadlock.. With this we should be able to resolve deadlocks..
				select {
				case chn <- email:
				case <-time.After(time.Millisecond * 100):
					fmt.Printf("WARNING!!! Stuck on sending email <<%s>> to %s, continuing...", email.String(), f.testName)
				}
			}
			svr.mu.Unlock()
			if smtpLog || smtpLogVerbose {
				fmt.Printf("SMTP: received Email (%d): %+v", i, email)
			}
		}
	}()

	t.Logf("SMTP: Server started and is listening on: %s", svr.ListenAddr)
	return svr
}

func (s *smtpServer) Stop(t *testing.T) {
	if s.socket == nil {
		return
	}
	t.Log("SMTP: Stopping SMTP test server")

	// Cancel SMTP context, to stop processing in all current connections
	s.stopFunc()

	// Stop listening socket to prevent new SMTP connections
	err := s.socket.Close()
	if err != nil {
		t.Logf("SMTP: error while closing SMTP socket: %v", err)
	}
	s.socket = nil

	// Close mailChan to allow go-routine reading from mailChan to terminate gracefully
	fmt.Print("SMTP: Closing mail channel")
	close(s.mailChan)

	// Wait for GO routines to terminate
	s.wg.Wait()

	// TODO: Check for unclaimed emails and fail test if any were found
}

func (s *smtpServer) AwaitEmail(ctx context.Context, testName, wantedFrom, wantedTo, wantedSubject string) <-chan *Email {
	// Return a buffered channel since tests normally don't start to listen on the channel before the method under
	// test have returned, but the channel is set up before the method is called.
	chn := make(chan *Email, 1)

	s.mu.Lock()
	if s.waitList == nil {
		s.waitList = make(map[chan *Email]filter)
	}
	s.waitList[chn] = filter{testName: testName, wantedFrom: wantedFrom, wantedTo: wantedTo, wantedSubject: wantedSubject}
	s.mu.Unlock()

	go func(chn chan *Email) {
		<-ctx.Done()
		s.mu.Lock()
		close(chn)
		delete(s.waitList, chn)
		s.mu.Unlock()
	}(chn)

	return chn
}

func (e *Email) String() string {
	return fmt.Sprintf("From: %s, To: %s, Subject: %s",
		e.From,
		e.To,
		e.Subject,
	)
}

func smtpListen(ctx context.Context, wg *sync.WaitGroup, ln net.Listener, config *tls.Config, mailChan chan<- *Email) {
	wg.Add(1)
	defer wg.Done()
	var conn net.Conn
	var err error
	var opErr *net.OpError
	for {
		conn, err = ln.Accept()
		if err != nil {
			if errors.As(err, &opErr) {
				if opErr.Err.Error() == "use of closed network connection" {
					fmt.Print("SMTP: server socket closed")
					return
				}
				fmt.Printf("SMTP: failed to accept connection: %v (inner error type %T)", err, opErr.Err)
			} else {
				fmt.Printf("SMTP: failed to accept connection: %v (%[1]T)", err)
			}
			continue
		}
		go func(conn net.Conn) {
			connCtx, connCancelFunc := context.WithTimeout(ctx, time.Second)
			defer conn.Close()
			defer connCancelFunc()
			smtpHandle(connCtx, conn, config, mailChan)
		}(conn)
	}
}

type smtpSender struct{ w io.Writer }

func (s smtpSender) send(f string) {
	if smtpLog || smtpLogVerbose {
		fmt.Printf("SMTP -> %s", f)
	}
	_, err := s.w.Write([]byte(f + "\r\n"))
	if err != nil {
		fmt.Printf("Failed to write message '%s' to client, error: %v", f, err)
	}
}

func smtpHandle(ctx context.Context, c net.Conn, config *tls.Config, mailChan chan<- *Email) {
	var tlsMode, rxData, qpEnc bool
	var line, mailFrom, mailTo, mailSubject, mailBody, qpBuff string
	send := smtpSender{c}.send
	send("220 127.0.0.1 ESMTP service ready")
	s := bufio.NewScanner(c)
	defer func() {
		if tlsMode {
			// Close the TLS connection before we exit, since we created it here. The original (received)
			// connection is closed by the caller.
			err := c.Close()
			if err != nil {
				fmt.Printf("SMTP: error while closing TLS socket connection: %v", err)
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			fmt.Print("SMTP: still receiving email while starting to shutdown SMTP server!!")
			return
		default:
		}
		if !s.Scan() {
			return
		}
		line = s.Text()
		parts := strings.SplitN(line, ":", 2)
		if !rxData {
			if smtpLog || smtpLogVerbose {
				fmt.Printf("SMTP <- %s", line)
			}
			switch parts[0] {
			case "EHLO localhost":
				send("250-127.0.0.1 ESMTP offers a warm hug of welcome")
				if !tlsMode {
					send("250-STARTTLS")
				}
				send("250 Ok")
			case "STARTTLS":
				send("220 Go ahead")
				// Switch over to a TLS socket, and update the c, s, send variables to use the new TLS socket.
				c = tls.Server(c, config)
				send = smtpSender{c}.send
				s = bufio.NewScanner(c)
				tlsMode = true
			case "MAIL FROM": // MAIL FROM:<joe1@example.com>
				send("250 Ok")
				mailFrom += parts[1]
			case "RCPT TO": // RCPT TO:<joe2@example.com>
				send("250 Ok")
				mailTo += parts[1]
			case "DATA":
				send("354 send the mail data, end with .")
				send("250 Ok")
				rxData = true
			case "QUIT":
				send("221 127.0.0.1 Service closing transmission channel")
				return
			default:
				fmt.Printf("SMTP: unrecognized command: %q (TLS: %v)", line, tlsMode)
			}
		} else {
			if smtpLogVerbose {
				fmt.Printf("SMTP <- %s", line)
			}
			switch parts[0] {
			case ".":
				if line == "." {
					if qpEnc && qpBuff != "" {
						qpReader := quotedprintable.NewReader(strings.NewReader(qpBuff))
						b, err := ioutil.ReadAll(qpReader)
						if err != nil {
							fmt.Printf("SMTP: Failed to decode quoted-printable part: %v", err)
						} else {
							mailBody += string(b)
						}
					}
					mailChan <- &Email{
						From:    mailFrom,
						To:      mailTo,
						Subject: mailSubject,
						Body:    mailBody,
					}
					rxData = false
					qpEnc = false
				}
			case "Subject": // Subject: test
				mailSubject = parts[1]
			default:
				if !qpEnc {
					mailBody += line + "\n"
				} else {
					qpBuff += line + "\r\n"
				}
			}
			if line == "Content-Transfer-Encoding: quoted-printable" {
				qpEnc = true
			}
		}
	}
}

// localhostCert is a PEM-encoded TLS cert generated from src/crypto/tls:
// go run generate_cert.go --rsa-bits 1024 --host 127.0.0.1,::1,example.com \
// 		--ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var localhostCert = []byte(`
-----BEGIN CERTIFICATE-----
MIICFDCCAX2gAwIBAgIRAK0xjnaPuNDSreeXb+z+0u4wDQYJKoZIhvcNAQELBQAw
EjEQMA4GA1UEChMHQWNtZSBDbzAgFw03MDAxMDEwMDAwMDBaGA8yMDg0MDEyOTE2
MDAwMFowEjEQMA4GA1UEChMHQWNtZSBDbzCBnzANBgkqhkiG9w0BAQEFAAOBjQAw
gYkCgYEA0nFbQQuOWsjbGtejcpWz153OlziZM4bVjJ9jYruNw5n2Ry6uYQAffhqa
JOInCmmcVe2siJglsyH9aRh6vKiobBbIUXXUU1ABd56ebAzlt0LobLlx7pZEMy30
LqIi9E6zmL3YvdGzpYlkFRnRrqwEtWYbGBf3znO250S56CCWH2UCAwEAAaNoMGYw
DgYDVR0PAQH/BAQDAgKkMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA8GA1UdEwEB/wQF
MAMBAf8wLgYDVR0RBCcwJYILZXhhbXBsZS5jb22HBH8AAAGHEAAAAAAAAAAAAAAA
AAAAAAEwDQYJKoZIhvcNAQELBQADgYEAbZtDS2dVuBYvb+MnolWnCNqvw1w5Gtgi
NmvQQPOMgM3m+oQSCPRTNGSg25e1Qbo7bgQDv8ZTnq8FgOJ/rbkyERw2JckkHpD4
n4qcK27WkEDBtQFlPihIM8hLIuzWoi/9wygiElTy/tVL3y7fGCvY2/k1KBthtZGF
tN8URjVmyEo=
-----END CERTIFICATE-----`)

// localhostKey is the private key for localhostCert.
var localhostKey = []byte(testingKey(`
-----BEGIN RSA TESTING KEY-----
MIICXgIBAAKBgQDScVtBC45ayNsa16NylbPXnc6XOJkzhtWMn2Niu43DmfZHLq5h
AB9+Gpok4icKaZxV7ayImCWzIf1pGHq8qKhsFshRddRTUAF3np5sDOW3QuhsuXHu
lkQzLfQuoiL0TrOYvdi90bOliWQVGdGurAS1ZhsYF/fOc7bnRLnoIJYfZQIDAQAB
AoGBAMst7OgpKyFV6c3JwyI/jWqxDySL3caU+RuTTBaodKAUx2ZEmNJIlx9eudLA
kucHvoxsM/eRxlxkhdFxdBcwU6J+zqooTnhu/FE3jhrT1lPrbhfGhyKnUrB0KKMM
VY3IQZyiehpxaeXAwoAou6TbWoTpl9t8ImAqAMY8hlULCUqlAkEA+9+Ry5FSYK/m
542LujIcCaIGoG1/Te6Sxr3hsPagKC2rH20rDLqXwEedSFOpSS0vpzlPAzy/6Rbb
PHTJUhNdwwJBANXkA+TkMdbJI5do9/mn//U0LfrCR9NkcoYohxfKz8JuhgRQxzF2
6jpo3q7CdTuuRixLWVfeJzcrAyNrVcBq87cCQFkTCtOMNC7fZnCTPUv+9q1tcJyB
vNjJu3yvoEZeIeuzouX9TJE21/33FaeDdsXbRhQEj23cqR38qFHsF1qAYNMCQQDP
QXLEiJoClkR2orAmqjPLVhR3t2oB3INcnEjLNSq8LHyQEfXyaFfu4U9l5+fRPL2i
jiC0k/9L5dHUsF0XZothAkEA23ddgRs+Id/HxtojqqUT27B8MT/IGNrYsp4DvS/c
qgkeluku4GjxRlDMBuXk94xOBEinUs+p/hwP1Alll80Tpg==
-----END RSA TESTING KEY-----`))

func testingKey(s string) string { return strings.ReplaceAll(s, "TESTING KEY", "PRIVATE KEY") }

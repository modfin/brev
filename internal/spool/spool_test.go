package spool

import (
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/envelope"
	"github.com/rs/xid"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setup() (*Spool, string, error) {
	dir, err := ioutil.TempDir("", "spool_test")
	if err != nil {
		return nil, "", err
	}
	spool, err := New(Config{Dir: dir}, nil)
	if err != nil {
		return nil, "", err
	}
	return spool, dir, nil
}

func teardown(dir string) {
	if strings.HasPrefix(dir, "/tmp") {
		fmt.Println("Removing dir", dir)
		err := os.RemoveAll(dir)
		if err != nil {
			fmt.Println("Failed to remove dir", err)
		}
	}
}

func validJob(eid xid.ID) Job {
	return Job{
		EID:       eid,
		MessageId: fmt.Sprintf("%s@localhost", eid.String()),
		From:      "from@localhost",
		Rcpt:      []string{"to@localhost"},
	}
}

func validEmail(eid xid.ID) io.Reader {
	//key, err := rsa.GenerateKey(rand.Reader, 2048)
	//if err != nil {
	//	panic(err)
	//}
	//pemBlock := &pem.Block{
	//	Type:  "RSA PRIVATE KEY",
	//	Bytes: x509.MarshalPKCS1PrivateKey(key),
	//}
	//pemBytes := pem.EncodeToMemory(pemBlock)
	//
	//sig, err := dkim.NewSigner(string(pemBytes))
	//if err != nil {
	//	panic(err)
	//}
	//
	//sig = sig.With(dkim.OpDomain("example.com")).With(dkim.OpSelector("test-selector"))
	//return sig
	eml := &brev.Email{
		Metadata: brev.Metadata{
			Id:         eid,
			ReturnPath: "test@example.com",
		},
		From: &brev.Address{
			Email: "from@example.com",
		},
		To: []*brev.Address{
			{
				Email: "to@example.com",
			},
		},
		Subject: "Test",
		Text:    "The content",
	}
	r, err := envelope.Marshal(eml, nil)
	if err != nil {
		panic(err)
	}
	return r
}

func TestEnqueueCreatesDirectoriesAndFiles(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	eid := xid.New()
	email := validJob(eid)
	mail := validEmail(eid)

	err = spool.Enqueue(email, mail)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	canonicalDir := tid2dir(spool.cfg.Dir, canonical, email.EID)
	queueDir := tid2dir(spool.cfg.Dir, queue, email.EID)

	if !fileExists(filepath.Join(canonicalDir, fmt.Sprintf("%s.eml", email.EID))) {
		t.Fatalf("Expected email file to be created in canonical directory")
	}

	if !fileExists(filepath.Join(queueDir, fmt.Sprintf("%s.job", email.EID))) {
		t.Fatalf("Expected job file to be created in queue directory")
	}
}

func TestDequeueMovesJobToProcessing(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	eid := xid.New()
	email := validJob(eid)
	mail := validEmail(eid)

	err = spool.Enqueue(email, mail)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	_, eml, err := spool.Dequeue(email.EID)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	defer eml.Close()

	procDir := tid2dir(spool.cfg.Dir, processing, email.EID)
	if !fileExists(filepath.Join(procDir, fmt.Sprintf("%s.job", email.EID))) {
		t.Fatalf("Expected job file to be moved to processing directory")
	}
}

func TestSucceedMovesJobToSent(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	eid := xid.New()
	email := validJob(eid)
	mail := validEmail(eid)

	err = spool.Enqueue(email, mail)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	_, eml, err := spool.Dequeue(email.EID)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	defer eml.Close()

	err = spool.Succeed(email.EID)
	if err != nil {
		t.Fatalf("Succeed failed: %v", err)
	}

	sentDir := tid2dir(spool.cfg.Dir, sent, email.EID)
	if !fileExists(filepath.Join(sentDir, fmt.Sprintf("%s.job", email.EID))) {
		t.Fatalf("Expected job file to be moved to sent directory")
	}
}

func TestFailMovesJobToFailed(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	eid := xid.New()
	email := validJob(eid)
	mail := validEmail(eid)

	err = spool.Enqueue(email, mail)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	_, eml, err := spool.Dequeue(email.EID)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	defer eml.Close()

	err = spool.Fail(email.EID)
	if err != nil {
		t.Fatalf("Fail failed: %v", err)
	}

	failDir := tid2dir(spool.cfg.Dir, failed, email.EID)
	if !fileExists(filepath.Join(failDir, fmt.Sprintf("%s.job", email.EID))) {
		t.Fatalf("Expected job file to be moved to failed directory")
	}
}

func TestLogfCreatesLogFile(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	eid := xid.New()
	err = spool.Logf(eid, "test log message")
	if err != nil {
		t.Fatalf("Logf failed: %v", err)
	}

	logDir := tid2dir(spool.cfg.Dir, log, eid)
	if !fileExists(filepath.Join(logDir, fmt.Sprintf("%s.log", eid.String()))) {
		t.Fatalf("Expected log file to be created")
	}
}

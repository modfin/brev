package spool

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/dkim"
	"github.com/rs/xid"
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
	spool, err := New(Config{Dir: dir})
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
			fmt.Println("Failed to remove dir", dir, err)
		}
	}
}

func validEmail() *brev.Email {
	return &brev.Email{
		Metadata: brev.Metadata{
			Id:         xid.New(),
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
		Text:    "The contentn",
	}
}

func validSigner() *dkim.Signer {

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	pemBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}
	pemBytes := pem.EncodeToMemory(pemBlock)

	sig, err := dkim.NewSigner(string(pemBytes))
	if err != nil {
		panic(err)
	}

	sig = sig.With(dkim.OpDomain("example.com")).With(dkim.OpSelector("test-selector"))
	return sig
}

func TestStart(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	spool.Start()
}

func TestStop(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	err = spool.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestEnqueue(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	email := validEmail()
	signer := validSigner()

	err = spool.Enqueue(email, signer)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
}

func TestEnqueueFailsOnInvalidDir(t *testing.T) {
	spool, _, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	spool.cfg.Dir = "/invalid/dir"

	email := validEmail()
	signer := validSigner()

	err = spool.Enqueue(email, signer)
	if err == nil {
		t.Fatalf("Enqueue should have failed on invalid directory")
	}
}

func TestDequeue(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	email := validEmail()
	signer := validSigner()

	err = spool.Enqueue(email, signer)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	job, eml, err := spool.Dequeue(email.Metadata.Id)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	defer eml.Close()

	if job.EID != email.Metadata.Id {
		t.Fatalf("Dequeue returned wrong job")
	}
}

func TestDequeueFailsOnMissingJob(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	_, _, err = spool.Dequeue(xid.New())
	if err == nil {
		t.Fatalf("Dequeue should have failed on missing job")
	}
}

func TestSucceed(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	email := validEmail()
	signer := validSigner()

	err = spool.Enqueue(email, signer)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	_, _, err = spool.Dequeue(email.Metadata.Id)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	err = spool.Succeed(email.Metadata.Id)
	if err != nil {
		t.Fatalf("Succeed failed: %v", err)
	}

	path := filepath.Join(eid2dir(spool.cfg.Dir, sent, email.Metadata.Id),
		fmt.Sprintf("%s.job", email.Metadata.Id.String()))
	if !fileExists(path) {
		t.Fatalf("Succeed failed to move job to sent")
	}

}

func TestSucceedFailsOnMissingJob(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	err = spool.Succeed(xid.New())
	if err == nil {
		t.Fatalf("Succeed should have failed on missing job")
	}
}

func TestFail(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	email := validEmail()
	signer := validSigner()

	err = spool.Enqueue(email, signer)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	_, _, err = spool.Dequeue(email.Metadata.Id)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	err = spool.Fail(email.Metadata.Id)
	if err != nil {
		t.Fatalf("Fail failed: %v", err)
	}

	path := filepath.Join(eid2dir(spool.cfg.Dir, failed, email.Metadata.Id),
		fmt.Sprintf("%s.job", email.Metadata.Id.String()))
	if !fileExists(path) {
		t.Fatalf("Failed failed to move job to failed")
	}
}

func TestFailFailsOnMissingJob(t *testing.T) {
	spool, dir, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer teardown(dir)

	err = spool.Fail(xid.New())
	if err == nil {
		t.Fatalf("Fail should have failed on missing job")
	}
}

func TestLogf(t *testing.T) {
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

	path := filepath.Join(eid2dir(spool.cfg.Dir, log, eid),
		fmt.Sprintf("%s.log", eid.String()))

	if !fileExists(path) {
		t.Fatalf("Logf failed to write log")
	}

	f, err := os.ReadFile(path)
	fmt.Println(string(f))
}

func TestLogfFailsOnInvalidDir(t *testing.T) {
	spool, _, err := setup()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	spool.cfg.Dir = "/invalid/dir"

	eid := xid.New()
	err = spool.Logf(eid, "test log message")
	if err == nil {
		t.Fatalf("Logf should have failed on invalid directory")
	}
}

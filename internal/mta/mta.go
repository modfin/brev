package mta

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/crholm/brev/dnsx"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"github.com/crholm/brev/internal/signals"
	"github.com/crholm/brev/smtpx"
	"github.com/crholm/brev/tools"
	"sync"
	"time"
)

// Mail Transfer Agent

func Init(ctx context.Context, db dao.DAO) (done chan interface{}) {
	fmt.Println("[MTA]: Starting MTA")
	done = make(chan interface{})
	once := sync.Once{}
	closer := func() {
		once.Do(func() {
			close(done)
		})
	}

	go func() {
		err := startMTA(ctx, db)
		if err != nil {
			fmt.Println("[MTA]: got error from return", err)
		}
		closer()
	}()

	return done
}

func loadDKIMKey() (*rsa.PrivateKey, error) {
	d, _ := pem.Decode([]byte(config.Get().DKIMPrivetKey))
	if d == nil {
		return nil, errors.New("could not decode private dkim key from config")
	}
	// try to parse it as PKCS1 otherwise try PKCS8
	var privateKey *rsa.PrivateKey
	if key, err := x509.ParsePKCS1PrivateKey(d.Bytes); err != nil {
		if key, err := x509.ParsePKCS8PrivateKey(d.Bytes); err != nil {
			return nil, errors.New("found no standard to decode private dkim key")
		} else {
			privateKey = key.(*rsa.PrivateKey)
		}
	} else {
		privateKey = key
	}
	privateKey.Precompute()
	return privateKey, nil
}

func startMTA(ctx context.Context, db dao.DAO) error {

	done := make(chan interface{})
	go func() {
		<-ctx.Done()
		fmt.Println("[MTA]: Shutting down mta server")
		close(done)
	}()

	size := config.Get().Workers

	spool := make(chan dao.SpoolEmail, size*2)

	for i := 0; i < size; i++ {
		go worker(spool, db)
	}

	newMailInSpool, cancel := signals.Listen(signals.NewMailInSpool)
	defer cancel()

	for {

		emails, err := db.GetQueuedEmails(size * 2)

		if err != nil {
			fmt.Println(err)
		}

		if len(emails) > 0 {
			fmt.Println("[MTA]: processing", len(emails), "in internal spool")
		}

		for _, email := range emails {
			err := db.ClaimEmail(email.MessageId)
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
		case <-done:
			return errors.New("mta ordered shutdown from context")
		}

	}
}

func worker(spool chan dao.SpoolEmail, db dao.DAO) {

	workerId := tools.RandStringRunes(5)

	fmt.Printf("[MTA-Worker %s]: Starting worker\n", workerId)
	for spoolmail := range spool {

		content, err := db.GetEmailContent(spoolmail.MessageId)

		transferlist := dnsx.LookupEmailMX(spoolmail.Recipients)
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
			conn, err := smtpx.NewConnection(addr, nil)
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

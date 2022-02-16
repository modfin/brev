package mta

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/crholm/brev"
	"github.com/crholm/brev/dkimx"
	"github.com/crholm/brev/dnsx"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"github.com/crholm/brev/internal/signals"
	"github.com/crholm/brev/smtpx"
	"github.com/crholm/brev/tools"
	"io"
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

	dkimKey, err := loadDKIMKey()
	if err != nil {
		panic(err) // Probably a bad ide
		return
	}

	fmt.Printf("[MTA-Worker %s]: Starting worker\n", workerId)
	for spoolmail := range spool {

		key, err := db.GetApiKey(spoolmail.ApiKey)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not retrieve key settings for api key %s, err %v\n", workerId, spoolmail.ApiKey, err)
			continue
		}

		/// Creating stmp message from brev mail, move to a marshal function in brev.Email ? ///
		var brevmail brev.Email
		err = json.Unmarshal(spoolmail.Content, &brevmail)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not unmarshal email content for mail %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}
		if brevmail.Headers == nil {
			brevmail.Headers = map[string][]string{}
		}
		// Should this be moved to the ingress?
		message := smtpx.NewMessage()

		mxDomain := config.Get().MXDomain
		if len(key.MxCNAME) > 3 {
			mxDomain = key.MxCNAME
		}
		brevmail.Headers["Return-Path"] = []string{fmt.Sprintf("<bounces_%s@%s>", spoolmail.MessageId, mxDomain)}
		brevmail.Headers["Message-ID"] = []string{fmt.Sprintf("<%s@%s>", spoolmail.MessageId, config.Get().Hostname)}
		brevmail.Headers["Date"] = []string{time.Now().In(time.UTC).Format(smtpx.MessageDateFormat)}

		for key, values := range brevmail.Headers {
			message.SetHeader(key, values...)
		}
		message.SetHeader("From", message.FormatAddress(brevmail.From.Email, brevmail.From.Name))
		for _, to := range brevmail.To {
			message.AddToHeader("To", message.FormatAddress(to.Email, to.Name))
		}
		for _, cc := range brevmail.Cc {
			message.AddToHeader("Cc", message.FormatAddress(cc.Email, cc.Name))
		}
		message.SetHeader("Subject", brevmail.Subject)

		if brevmail.HTML != "" {
			message.SetBody("text/html", brevmail.HTML)
			if brevmail.Text != "" {
				message.AddAlternative("text/plain", brevmail.Text)
			}
		}
		if brevmail.HTML == "" && brevmail.Text != "" {
			message.SetBody("text/plain", brevmail.Text)
		}

		for _, att := range brevmail.Attachments {
			content, err := base64.StdEncoding.DecodeString(att.Content)
			if err != nil {
				fmt.Printf("[MTA-Worker %s] could not base64 decode attachement for %s, err %v\n", workerId, spoolmail.MessageId, err)
				continue
			}
			message.Attach(att.Filename, smtpx.Rename(att.Filename), smtpx.SetCopyFunc(func(writer io.Writer) error {
				_, err := io.Copy(writer, bytes.NewBuffer(content))
				return err
			}))
		}

		rawBuffer := bytes.NewBuffer(nil)
		_, err = message.WriteTo(rawBuffer)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not write mail to buffer %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}
		rawMessage := rawBuffer.Bytes()

		/// Signing email with DKIM
		ops := dkimx.NewSigOptions()
		ops.Canonicalization = "relaxed/relaxed"
		ops.Domain = key.Domain
		ops.Selector = config.Get().DKIMSelector
		ops.Headers = []string{"from",
			"mime-version",
			"date",
			"message-id",
			"subject",
			"content-type",
			"return-path",
		}
		if len(brevmail.To) > 0 {
			ops.Headers = append(ops.Headers, "to")
		}
		if len(brevmail.Cc) > 0 {
			ops.Headers = append(ops.Headers, "cc")
		}

		err = dkimx.Sign(&rawMessage, ops, dkimKey)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not dkim sign mail %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}

		////// Figuring out recipients /////
		var emails []string
		for _, a := range brevmail.To {
			emails = append(emails, a.Email)
		}
		for _, a := range brevmail.Cc {
			emails = append(emails, a.Email)
		}
		for _, a := range brevmail.Bcc {
			emails = append(emails, a.Email)
		}

		transferlist := dnsx.LookupEmailMX(emails)
		if err != nil {
			fmt.Printf("[MTA-Worker %s] could not look up TransferList server for recipiants of mail %s, err %v\n", workerId, spoolmail.MessageId, err)
			continue
		}

		fmt.Printf("\n%+v\n", transferlist)

		for _, mx := range transferlist {
			if len(mx.MXServers) == 0 {
				fmt.Printf("[MTA-Worker %s]: could not find mx server for %v\n", workerId, mx.Emails)
				continue
			}
			addr := mx.MXServers[0] + ":25"
			fmt.Printf("[MTA-Worker %s]: Transferring emails to %s domain through %s\n", workerId, mx.Domain, addr)
			err = smtpx.SendMail(addr, nil, brevmail.From.Email, mx.Emails, bytes.NewBuffer(rawMessage))
			if err != nil {
				fmt.Printf("[MTA-Worker %s]: could not transfer mail to %s for mail %s\n", workerId, addr, spoolmail.MessageId)
				continue
			}
			fmt.Printf("[MTA-Worker %s]: Transfer compleat for mail %s\n", workerId, spoolmail.MessageId)
		}

	}
}

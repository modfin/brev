package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/brev/smtpx/envelope"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/mon"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	app := &cli.App{
		Name:  "brev",
		Usage: "a cli that send email directly to the mx servers and other mail utilities",

		Commands: []*cli.Command{
			{
				Name: "gen-dkim-keys",
				Flags: []cli.Flag{
					&cli.IntFlag{Name: "key-size", Value: 2048},
					&cli.StringFlag{Name: "out", Value: "-"},
				},
				Action: gendkim,
			},
		},

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "subject",
				Usage: "Set subject line",
			},
			&cli.StringFlag{
				Name:  "from",
				Usage: "Set from email, 'email' or 'name <email>' is valid",
			},
			&cli.StringFlag{
				Name:  "message-id",
				Usage: "set the message id",
			},
			&cli.StringSliceFlag{
				Name:  "to",
				Usage: "Set 'to' email, 'email' or 'name <email>' is valid",
			},
			&cli.StringSliceFlag{
				Name:  "cc",
				Usage: "Set cc email, 'email' or 'name <email>' is valid",
			},
			&cli.StringSliceFlag{
				Name:  "header",
				Usage: "Set a header, format 'key: value'",
			},
			&cli.StringSliceFlag{
				Name:  "bcc",
				Usage: "Set cc email, 'email' or 'name <email>' is valid",
			},
			&cli.StringFlag{
				Name:  "text",
				Usage: "text content of the mail",
			},
			&cli.StringFlag{
				Name:  "html",
				Usage: "html content of the mail",
			},
			&cli.StringSliceFlag{
				Name:  "attach",
				Usage: "path to file attachment",
			},
			&cli.StringFlag{
				Name:  "msa",
				Usage: "use specific Mail Submission Agent, eg example.com:587",
			},
			&cli.IntFlag{
				Name:  "msa-port",
				Usage: "will try to use specific port for the MSA server",
			},
			&cli.StringFlag{
				Name:  "msa-user",
				Usage: "username for the msa server",
			},
			&cli.StringFlag{
				Name:  "msa-pass",
				Usage: "password for the msa server",
			},
			&cli.BoolFlag{
				Name: "verbose",
			},
		},
		Action: sendmail,
	}

	err := app.Run(os.Args)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "got err", err)
		os.Exit(1)
	}
}

func gendkim(c *cli.Context) (err error) {
	keysize := c.Int("key-size")
	privatekey, err := rsa.GenerateKey(rand.Reader, keysize)
	if err != nil {
		return err
	}

	var privateKeyBytes = x509.MarshalPKCS1PrivateKey(privatekey)
	privateKeyBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	}

	out := c.String("out")

	var privatePemFile = os.Stdout
	if out != "-" {
		err = os.MkdirAll(c.String("out"), 0755)
		if err != nil {
			fmt.Printf("error when create directory: %s \n", err)
			return err
		}
		privatePemFile, err = os.Create(filepath.Join(c.String("out"), "dkim-private.pem"))
		if err != nil {
			fmt.Printf("error when create dkim-private.pem: %s \n", err)
			return err
		}
		defer privatePemFile.Close()
	}

	if privatePemFile == os.Stdout {
		_, _ = os.Stdout.WriteString("==== Private Key ====\n")
	}

	err = pem.Encode(privatePemFile, privateKeyBlock)
	if err != nil {
		fmt.Printf("error when encode private pem: %s \n", err)
		return err
	}

	if privatePemFile == os.Stdout {
		_, _ = os.Stdout.WriteString("\n==== DNS Record ====\n")
	}

	publickey := &privatekey.PublicKey
	publickeyBytes, err := x509.MarshalPKIXPublicKey(publickey)
	if err != nil {
		return err
	}
	pubRecord := fmt.Sprintf("v=DKIM1; k=rsa; p=%s;", base64.StdEncoding.EncodeToString(publickeyBytes))

	var pubRecordFile = os.Stdout
	if out != "-" {
		pubRecordFile, err = os.Create(filepath.Join(c.String("out"), "dkim-pub.dns.txt"))
		if err != nil {
			return err
		}
		defer pubRecordFile.Close()
	}

	_, err = pubRecordFile.Write([]byte(pubRecord))
	if err != nil {
		return err
	}

	if privatePemFile == os.Stdout {
		_, _ = os.Stdout.WriteString("\n")

	}

	return nil
}

type printer struct{}

func (p printer) Logf(format string, args ...interface{}) error {
	logrus.Infof(format, args...)
	return nil
}

func sendmail(c *cli.Context) (err error) {

	subject := c.String("subject")

	from, err := brev.ParseAddress(c.String("from"))
	if err != nil || from == nil || from.Email == "" {
		from = &brev.Address{}
		from.Email, err = tools.SystemUri()
		if err != nil {
			return err
		}
	}

	messageId := c.String("message-id")

	parseAddr := func(a string) mon.Result[*brev.Address] {
		add, err := brev.ParseAddress(a)
		if err == nil {
			return mon.Ok[*brev.Address](add)
		}
		return mon.Err[*brev.Address](err)
	}

	toR := slicez.Map(c.StringSlice("to"), parseAddr)
	ccR := slicez.Map(c.StringSlice("cc"), parseAddr)
	bccR := slicez.Map(c.StringSlice("bcc"), parseAddr)
	e, found := slicez.Find(slicez.Concat(toR, ccR, bccR), func(m mon.Result[*brev.Address]) bool {
		return !m.Ok()
	})
	if found {
		fmt.Println("emails:123123", e.Error(), e.MustGet())
		return e.Error()
	}

	to := slicez.Map(toR, func(a mon.Result[*brev.Address]) *brev.Address { return a.OrEmpty() })
	cc := slicez.Map(ccR, func(a mon.Result[*brev.Address]) *brev.Address { return a.OrEmpty() })
	bcc := slicez.Map(bccR, func(a mon.Result[*brev.Address]) *brev.Address { return a.OrEmpty() })

	headers := c.StringSlice("header")

	text := c.String("text")
	html := c.String("html")
	attachments := c.StringSlice("attach")
	msaServer := c.String("msa")
	msaUser := c.String("msa-user")
	msaPass := c.String("msa-pass")

	message := envelope.NewEnvelope()
	if len(messageId) == 0 {
		messageId = smtpx.GenerateId()
	}

	message.SetHeader("Message-ID", fmt.Sprintf("<%s>", messageId))
	message.SetHeader("Subject", subject)
	message.SetHeader("From", from.String())

	var emails []*brev.Address

	tostr := func(s *brev.Address) string {
		return s.String()
	}

	if len(to) > 0 {
		message.SetHeader("To", slicez.Map(to, tostr)...)
		emails = append(emails, to...)
	}
	if len(cc) > 0 {
		message.SetHeader("Cc", slicez.Map(cc, tostr)...)
		emails = append(emails, cc...)
	}
	if len(bcc) > 0 {
		emails = append(emails, bcc...)
	}
	for _, h := range headers {
		parts := strings.SplitN(h, ": ", 2)
		if len(parts) != 2 {
			return errors.New("header, " + h + ", is not correctly formatted")
		}
		message.SetHeader(parts[0], parts[1])
	}

	if len(emails) == 0 {
		return errors.New("there has to be at least 1 email to send to, cc or bcc")
	}

	// Uggly control flow code
	if len(text) > 0 && len(html) > 0 {
		// order is important, and apparently the text/plain part should go first in the multipart/alternative
		// at least gmail picks the last alternative to display...
		message.SetBody("text/plain", text)
		message.AddAlternative("text/html", html)
	} else if len(html) > 0 {
		message.SetBody("text/html", html)
	} else if len(text) > 0 {
		message.SetBody("text/plain", text)
	}

	for _, a := range attachments {
		message.Attach(a)
	}

	tomail := func(s *brev.Address) string {
		return s.Email
	}
	emailstrs := slicez.Map(emails, tomail)
	if msaServer != "" {
		var auth smtpx.Auth

		if msaUser != "" && msaPass != "" {
			auth = smtpx.PlainAuth("", msaUser, msaPass, strings.Split(msaServer, ":")[0])
		}

		return smtpx.SendMail(nil, msaServer, "localhost", auth, from.Email, emailstrs, message)
	}

	transferlist := LookupEmailMX(emailstrs)

	if len(transferlist) == 0 {
		return errors.New("could not find any mx server to send mails to")
	}

	var logger smtpx.Logger = &printer{}

	for _, mx := range transferlist {
		if len(mx.MXServers) == 0 {
			fmt.Println("could not find mx server for", mx.Emails)
			continue
		}

		mx.Emails = slicez.Uniq(mx.Emails)
		addr := mx.MXServers[0]
		logger.Logf("Transferring emails for %s to mx smtp://%s", mx.Domain, addr)
		for _, t := range mx.Emails {
			logger.Logf(" - %s", t)
		}

		from := from.Email

		err = smtpx.SendMail(compare.Ternary(c.Bool("verbose"), logger, nil), addr, "localhost", nil, from, mx.Emails, message)
		if err != nil {
			return
		}
	}

	return nil
}

type TransferList struct {
	Domain    string
	Emails    []string
	MXServers []string
	Err       error
}

var isIPAddress = regexp.MustCompile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")

type MXLookup func(emails []string) []TransferList

func LookupEmailMX(emails []string) []TransferList {
	var mx []TransferList

	emails = slicez.Map(emails, strings.ToLower)
	var buckets = slicez.GroupBy(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	})
	delete(buckets, "")

	for domain, addresses := range buckets {

		var err error
		var recs []*net.MX
		if isIPAddress.MatchString(domain) {
			recs = append(recs, &net.MX{
				Host: domain,
			})
		}
		if len(recs) == 0 {
			recs, _ = net.LookupMX(domain) // TODO: error?
		}

		slicez.SortBy(recs, func(i, j *net.MX) bool {
			return i.Pref < j.Pref
		})
		var servers = slicez.Map(recs, func(rec *net.MX) string {
			return rec.Host + ":25"
		})

		mx = append(mx, TransferList{
			Domain:    domain,
			Emails:    addresses,
			MXServers: servers,
			Err:       err,
		})

	}
	return mx

}

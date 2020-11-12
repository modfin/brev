package main

import (
	"errors"
	"fmt"
	"github.com/crholm/brev/internal/dnsx"
	"github.com/crholm/brev/internal/smtpx"
	"github.com/crholm/brev/internal/tools"
	"github.com/urfave/cli/v2"
	"os"
	"strings"
)

func main() {
	app := &cli.App{
		Name:  "brev",
		Usage: "a cli that send email directly to the mx servers",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "subject",
				Usage: "Set subject line",
			},
			&cli.StringFlag{
				Name:  "from",
				Usage: "Set from email",
			},
			&cli.StringFlag{
				Name:  "message-id",
				Usage: "set the message id",
			},
			&cli.StringSliceFlag{
				Name:  "to",
				Usage: "Set to email",
			},
			&cli.StringSliceFlag{
				Name:  "cc",
				Usage: "Set cc email",
			},
			&cli.StringSliceFlag{
				Name:  "header",
				Usage: "Set a header, format 'key: value'",
			},
			&cli.StringSliceFlag{
				Name:  "bcc",
				Usage: "Set cc email",
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
			&cli.StringFlag{
				Name:  "msa-user",
				Usage: "username for the msa server",
			},
			&cli.StringFlag{
				Name:  "msa-pass",
				Usage: "password for the msa server",
			},

		},
		Action: run,
	}

	err := app.Run(os.Args)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "got err", err)
		os.Exit(1)
	}
}

func run(c *cli.Context) (err error) {

	subject := c.String("subject")
	from := c.String("from")
	messageId := c.String("message-id")

	to := c.StringSlice("to")
	cc := c.StringSlice("cc")
	bcc := c.StringSlice("bcc")
	headers := c.StringSlice("header")

	text := c.String("text")
	html := c.String("html")
	attachments := c.StringSlice("attach")
	msaServer := c.String("msa")
	msaUser := c.String("msa-user")
	msaPass := c.String("msa-pass")

	message := smtpx.NewMessage()

	if len(from) == 0 {
		from, err = tools.SystemUri()
		if err != nil {
			return err
		}
	}

	if len(messageId) == 0 {
		messageId, err = smtpx.GenerateId()
		if err != nil {
			return err
		}
	}

	message.SetHeader("Message-ID", fmt.Sprintf("<%s>", messageId))
	message.SetHeader("Subject", subject)
	message.SetHeader("From", from)

	var emails []string
	if len(to) > 0 {
		message.SetHeader("To", to...)
		emails = append(emails, to...)
	}
	if len(cc) > 0 {
		message.SetHeader("Cc", cc...)
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

	if len(text) > 0 {
		message.SetBody("text/plain", text)
	}
	if len(html) > 0 {
		message.SetBody("text/html", html)
		if len(text) > 0 {
			message.AddAlternative("text/plain", text)
		}
	}

	for _, a := range attachments {
		message.Attach(a)
	}

	if msaServer != "" {
		var auth smtpx.Auth

		if msaUser != "" && msaPass != ""{
			auth = smtpx.PlainAuth("", msaUser, msaPass, strings.Split(msaServer, ":")[0])
		}

		return smtpx.SendMail(msaServer, auth, from, emails, message)
	}

	mxes, err := dnsx.LookupEmailMX(emails)
	if err != nil {
		return err
	}

	if len(mxes) == 0 {
		return errors.New("could not find any mx server to send mails to")
	}

	for _, mx := range mxes {
		mx.Emails = tools.Uniq(mx.Emails)
		addr := mx.MX + ":25"
		fmt.Println("Transferring emails for", mx.Domain, "to mx", "smtp://"+addr)
		for _, t := range mx.Emails {
			fmt.Println(" - ", t)
		}

		err = smtpx.SendMail(addr, nil, from, mx.Emails, message)
		if err != nil {
			return
		}
	}

	return nil
}

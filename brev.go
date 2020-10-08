package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/urfave/cli/v2"
	"gopkg.in/gomail.v2"
	"math"
	"math/big"
	"net"
	"net/smtp"
	"net/url"
	"os"
	"os/user"
	"time"
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

	text := c.String("text")
	html := c.String("html")
	attachments := c.StringSlice("attach")

	m := gomail.NewMessage()

	if len(from) == 0 {
		from, err = getSystemUri()
		if err != nil {
			return err
		}
	}

	if len(messageId) == 0 {
		messageId, err = generateId()
		if err != nil {
			return err
		}
	}

	m.SetHeader("Message-ID", fmt.Sprintf("<%s>", messageId))
	m.SetHeader("Subject", subject)
	m.SetHeader("From", from)

	var aggTo []string
	if len(to) > 0 {
		m.SetHeader("To", to...)
		aggTo = append(aggTo, to...)
	}
	if len(cc) > 0 {
		m.SetHeader("Cc", cc...)
		aggTo = append(aggTo, cc...)
	}
	if len(bcc) > 0 {
		aggTo = append(aggTo, bcc...)
	}

	if len(text) > 0 {
		m.SetBody("text/plain", text)
	}
	if len(html) > 0 {
		m.SetBody("text/html", html)
	}

	for _, a := range attachments {
		m.Attach(a)
	}

	mxes, err := lookupMX(aggTo)

	buff := &bytes.Buffer{}
	_, err = m.WriteTo(buff)
	if err != nil {
		return
	}

	for _, mx := range mxes {
		mx.To = uniq(mx.To)
		fmt.Println("Transferring emails for", mx.Domain, "to mx", mx.MX)
		for _, t := range mx.To {
			fmt.Println(" - ", t)
		}

		err = smtp.SendMail(mx.MX+":25", nil, from, mx.To, buff.Bytes())
		if err != nil {
			return
		}
	}

	return nil
}

type MX struct {
	To     []string
	Domain string
	MX     string
	IP     string
}

func lookupMX(tos []string) (map[string]MX, error) {
	var mx = make(map[string]MX)

	for _, to := range tos {
		u, err := url.Parse("smtp://" + to)
		if err != nil {
			return nil, err
		}
		m, ok := mx[u.Host]
		if !ok {
			mx[u.Host] = MX{}
		}
		m = mx[u.Host]
		m.To = append(m.To, to)
		mx[u.Host] = m

		if m.MX != "" {
			continue
		}

		recs, err := net.LookupMX(u.Host)
		if err != nil {
			return nil, err
		}

		for _, r := range recs {
			ips, err := net.LookupIP(r.Host)
			if err != nil {
				return nil, err
			}

			if len(ips) > 0 {
				m.MX = r.Host
				m.Domain = u.Host
				mx[u.Host] = m
				break
			}
		}
	}
	return mx, nil
}

func getSystemUri() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	username := "unknown"
	u, err := user.Current()
	if err == nil {
		username = u.Username
	}
	return fmt.Sprintf("%s@%s", username, hostname), nil
}

func generateId() (string, error) {
	random, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return "", nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	pid := os.Getpid()
	nanoTime := time.Now().UTC().UnixNano()

	return fmt.Sprintf("%d.%d.%d@%s", nanoTime, pid, random, hostname), nil
}

func uniq(strs []string) []string {
	set := make(map[string]struct{})

	for _, s := range strs {
		set[s] = struct{}{}
	}
	var res []string
	for s := range set {
		res = append(res, s)
	}
	return res
}

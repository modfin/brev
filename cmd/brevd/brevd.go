package main

import (
	"context"
	"fmt"
	"github.com/modfin/brev/internal/clix"
	"github.com/modfin/brev/internal/dnsx"
	"github.com/modfin/brev/internal/mta"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/internal/web"
	"github.com/modfin/brev/smtpx/envelope/signer"
	"github.com/modfin/brev/tools"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {

	app := &cli.App{
		Name:   "brevd",
		Usage:  "a service for sending emails",
		Flags:  []cli.Flag{},
		Action: start,

		Commands: []*cli.Command{
			{
				Name:   "serve",
				Action: start,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name: "dev",
					},

					&cli.StringFlag{
						Name:    "default-host",
						EnvVars: []string{"BREV_DEFAULT_HOST"},
						Usage: "the default host name, the host name that the MX records points to. eg. the 'brev-server.com' in the following case\n" +
							"dig MX example.com\n" +
							"example.com.     60  IN   CNAME   mx.example.com\n" +
							"mx.example.com.  60  IN   MX      10 brev-server.com",
					},
					&cli.StringFlag{
						Name:    "http-interface",
						EnvVars: []string{"BREV_HTTP_INTERFACE"},
					},
					&cli.IntFlag{
						Name:    "http-port",
						Value:   8080,
						EnvVars: []string{"BREV_HTTP_PORT"},
					},

					&cli.StringFlag{
						Name:    "spool-dir",
						Value:   "./spool",
						EnvVars: []string{"BREV_SPOOL_DIR"},
						Usage:   "the directory where the email spool will be stored",
					},

					&cli.IntFlag{
						Name:    "mta-pool-size",
						Value:   100,
						EnvVars: []string{"BREV_MTA_POOL_SIZE"},
						Usage:   "how many concurrent workers to use for sending emails",
					},

					&cli.IntFlag{
						Name:    "mta-retry",
						Value:   7,
						EnvVars: []string{"BREV_MTA_RETRY"},
						Usage:   "for recoverable errors, how many times should brev retry sending the email. eg gray in cases of listing, status 4xx errors",
					},
					&cli.DurationFlag{
						Name:    "mta-retry-duration",
						Value:   24 * time.Hour,
						EnvVars: []string{"BREV_MTA_RETRY_DURATION"},
						Usage:   "brev will use a exponential backoff strategy for retrying emails, this is the duration between the first and last attempt",
					},

					&cli.DurationFlag{
						Name:    "mta-router-max-wait",
						Value:   2 * time.Minute,
						EnvVars: []string{"BREV_MTA_WAIT_FOR_CONNECTION"},
						Usage:   "how long should a brev job wait for a connection to a mx server before giving up and reschedule if crowded",
					},

					&cli.IntFlag{
						Name:    "mta-router-default-concurrency",
						Value:   5,
						EnvVars: []string{"BREV_MTA_ROUTER_DEFAULT_CONCURRENCY"},
						Usage: `how many concurrent connection can brev open to a mail server. Ie. it set to 5, brev can open 5 connections to the same mail
server, such as aspmx.l.google.com. Having to many concurrent connectons may result in emails beeing marked as spam. 
5-10 seems like a good number to start with and can probably be increased once the IP is warmed up`,
					},

					&cli.StringSliceFlag{
						Name:    "mta-router-server-concurrency",
						EnvVars: []string{"BREV_MTA_ROUTER_SERVER_CONCURRENCY"},
						Usage: `"used to set concurrency for specific mx servers, format aspmx.l.google.com=50 will set concurrency connection to 
aspmx.l.google.com to 50`,
					},

					&cli.StringFlag{
						Name:     "dkim-domain",
						Required: true,
						EnvVars:  []string{"BREV_DKIM_DOMAIN"},
						Usage:    "the domain to sign emails with, eg. 'brev-server.com'",
					},
					&cli.StringFlag{
						Name:     "dkim-selector",
						Required: true,
						EnvVars:  []string{"BREV_DKIM_SELECTOR"},
						Usage:    "the selector to use for signing emails, eg. 'test', resulting in DNS/TXT test._domainkey.brev-server.com containing public key",
					},
					&cli.StringFlag{
						Name:    "dkim-key",
						EnvVars: []string{"BREV_DKIM_KEY"},
						Usage:   "the private key to use for signing emails, a armored PEM file",
					},
					&cli.StringFlag{
						Name:    "dkim-key-file",
						EnvVars: []string{"BREV_DKIM_KEY_FILE"},
						Usage:   "the private key to use for signing emails, a file path to a armored PEM file",
					},
					&cli.StringFlag{
						Name:  "dkim-canonicalization-header",
						Usage: "can be 'simple' or 'relaxed'",
						Value: "relaxed",
						Action: func(c *cli.Context, s string) error {
							if s != "simple" && s != "relaxed" {
								return fmt.Errorf("invalid value for dkim-canonicalization-header, must be 'simple' or 'relaxed'")
							}
							return nil
						},
					},
					&cli.StringFlag{
						Name:  "dkim-canonicalization-body",
						Usage: "can be 'simple' or 'relaxed'",
						Value: "relaxed",
						Action: func(c *cli.Context, s string) error {
							if s != "simple" && s != "relaxed" {
								return fmt.Errorf("invalid value dkim-canonicalization-body, must be 'simple' or 'relaxed'")
							}
							return nil
						},
					},

					&cli.StringFlag{
						Name:  "dns-resolver",
						Usage: "ip address (and port) of the dns server/resolver to use for resolving DNS records",
						Value: "1.1.1.1:53",
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func start(c *cli.Context) error {

	var cfg = clix.Parse[Config](c)

	l := log.New()
	l.Formatter = &log.TextFormatter{
		ForceColors: true,
	}

	if cfg.Dev {
		l.SetLevel(log.DebugLevel)
	}

	lc := tools.LoggerCloner(l)

	l = lc.New("brevd")

	var stopServer func()
	c.Context, stopServer = context.WithCancel(c.Context)
	defer stopServer()

	l.Infof("Starting brevd")
	if cfg.Dev {
		l.Warn("Running in dev mode")
	}

	var services []Stoppable

	spool, err := spool.New(cfg.Spool, lc)
	if err != nil {
		return err
	}
	services = append(services, spool)

	var dnsxc dnsx.Client
	dnsxc = dnsx.New(cfg.DNSX, lc)
	services = append(services, dnsxc)
	if cfg.Dev {
		dnsxc = dnsx.NewMock("mailhog:1025")
		l.Warning("Dev mode, using mailhog as DNS resolver")
	}

	mta := mta.New(cfg.MTA, spool, dnsxc, lc)
	services = append(services, mta)

	// TODO validate that private key matches up with public key in DNS record provided by domain and selector
	cfg.Web.Signer, err = signer.New(cfg.DKIM)
	if err != nil {
		return fmt.Errorf("could not create signer: %w", err)
	}

	websrv := web.New(c.Context, cfg.Web, spool, lc)
	services = append(services, websrv)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sigc
	log.Infof("Got signal: %s, shutting down", sig)

	shutdownCtx, _ := context.WithTimeout(c.Context, 30*time.Second)

	wg := &sync.WaitGroup{}
	for _, service := range services {
		wg.Add(1)
		service := service
		go func(service Stoppable) {
			defer wg.Done()
			err := service.Stop(shutdownCtx)
			if err != nil {
				l.WithError(err).Error("Failed to stop service")
			}
		}(service)

	}

	go func() {
		<-shutdownCtx.Done()
		l.WithError(shutdownCtx.Err()).Warn("Shutdown was forced, terminating now")
		os.Exit(1)
	}()

	wg.Wait()
	l.Infof("Shutdown complete, terminating now")
	os.Exit(1)

	return nil
}

type Stoppable interface {
	Stop(ctx context.Context) error
}

type Config struct {
	Dev bool `cli:"dev"`

	Web   web.Config
	Spool spool.Config
	DKIM  signer.Config
	MTA   mta.Config
	DNSX  dnsx.Config
}

package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"github.com/modfin/brev/internal/clix"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/internal/web"
	"github.com/modfin/brev/smtpx/envelope/kim"
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
					},

					&cli.StringFlag{
						Name:    "dkim-key",
						Value:   "./spool",
						EnvVars: []string{"BREV_SPOOL_DIR"},
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

	fmt.Printf("%+v\n", cfg)

	l := log.New()

	l.AddHook(tools.LoggerWho{Name: "brevd"})

	var stopServer func()
	c.Context, stopServer = context.WithCancel(c.Context)
	defer stopServer()

	l.Infof("Starting brevd")

	var services []Stoppable

	spool, err := spool.New(cfg.Spool)
	if err != nil {
		return err
	}
	spool.Start()
	services = append(services, spool)

	pk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("could not generate rsa key: %w", err)
	}
	cfg.Web.Signer, err = kim.New(&kim.SignOptions{
		Domain:                 "example.com",
		Selector:               "test",
		Signer:                 pk,
		Hash:                   crypto.SHA256,
		HeaderCanonicalization: "relaxed",
		BodyCanonicalization:   "relaxed",
	})
	if err != nil {
		return fmt.Errorf("could not create signer: %w", err)
	}

	ht := web.New(c.Context, cfg.Web, spool)
	ht.Start()
	services = append(services, ht)

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
	Web   web.Config
	Spool spool.Config
}

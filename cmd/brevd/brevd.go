package main

import (
	"context"
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
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func start(c *cli.Context) error {
	l := log.New()

	l.AddHook(tools.LoggerWho{Name: "brevd"})

	var stopServer func()
	c.Context, stopServer = context.WithCancel(c.Context)
	defer stopServer()

	l.Infof("Starting server")

	var services []Stoppable

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

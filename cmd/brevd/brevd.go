package main

import (
	"fmt"
	"github.com/crholm/brev/dnsx"
	"github.com/crholm/brev/internal/api"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"github.com/crholm/brev/internal/mta"
	"github.com/crholm/brev/smtpx"
	"golang.org/x/net/context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	cfg := config.Get()

	db, err := dao.NewSQLite(cfg.DbURI)
	if err != nil {
		fmt.Println("could not connect to db", cfg.DbURI)
		fmt.Println(err)
		os.Exit(1)
	}

	apiDone := api.Init(ctx, db)
	transferAgent := mta.New(ctx, db, dnsx.LookupEmailMX, smtpx.NewConnection)
	transferAgent.Start(5)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	select {
	case sig := <-sigc:
		fmt.Println("SIGNAL:", sig.String())
	case <-apiDone:
		fmt.Println("[Brev]: Unexpected closing of api server")
	case <-transferAgent.Done():
		fmt.Println("[Brev]: Unexpected closing of mta server")
	}

	fmt.Println("[Brev]: Initiating server shutdown")
	cancel()
	select {
	case <-apiDone:
	case <-time.After(10 * time.Second):
	}
	fmt.Println("[Brev]: Shutdown complete, terminating now")
}

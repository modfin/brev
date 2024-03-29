package main

import (
	"context"
	"fmt"
	"github.com/modfin/brev/internal/api"
	"github.com/modfin/brev/internal/config"
	"github.com/modfin/brev/internal/dao"
	"github.com/modfin/brev/internal/hooker"
	"github.com/modfin/brev/internal/msa"
	"github.com/modfin/brev/internal/mta"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/henry/chanz"
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

	for _, key := range cfg.APIKeys {
		err := db.EnsureApiKey(key)
		if err != nil {
			fmt.Println("could not ensure api-key, err:", err)
			os.Exit(1)
		}
	}

	apiDone := api.Init(ctx, db, cfg)
	_, err = msa.New(ctx, db, config.Get())
	if err != nil {
		fmt.Println("could not start MSA", cfg.DbURI)
		fmt.Println(err)
		os.Exit(1)
	}

	hook := hooker.New(ctx, db)
	hook.Start(2)

	transferAgent := mta.New(ctx, db, smtpx.NewConnection, cfg.Hostname)
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
	case <-hook.Done():
		fmt.Println("[Brev]: Unexpected closing of posthook sender")
	}

	fmt.Println("[Brev]: Initiating server shutdown")
	cancel()
	select {
	case <-chanz.EveryDone(apiDone, transferAgent.Done(), hook.Done()):
	case <-time.After(10 * time.Second):
	}
	fmt.Println("[Brev]: Shutdown complete, terminating now")
}

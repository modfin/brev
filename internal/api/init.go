package api

import (
	"context"
	"fmt"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"github.com/crholm/brev/smtpx/dkim"
	"github.com/labstack/echo-contrib/prometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"sync"
	"time"
)

func Init(ctx context.Context, db dao.DAO) (done chan interface{}) {
	fmt.Println("[API]: Starting API")

	done = make(chan interface{})
	once := sync.Once{}
	closer := func() {
		once.Do(func() {
			close(done)
		})
	}

	signer, err := dkim.NewSigner(config.Get().DKIMPrivetKey)
	if err != nil {
		fmt.Println("[API]: Failed to load dkim private key, emails wont be signed using dkim, err;", err)
	}

	e := echo.New()

	prom := prometheus.NewPrometheus("echo", nil)
	e.Use(middleware.Logger(), prom.HandlerFunc)

	e.POST("/mta", EnqueueMTA(db, signer))
	//e.GET("/mta")            // returns list of sent emails
	//e.GET("/mta/:messageId") // returns log of specific email

	go func() {
		<-ctx.Done()
		fmt.Println("[API]: Shutting down api server")
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := e.Shutdown(shutdown)
		if err != nil {
			fmt.Println("[API]: shutdown err,", err)
		}
		closer()
	}()

	go func() {
		err := e.Start(":1234")
		if err != nil {
			fmt.Println("[API]: stopped err,", err)
		}
		closer()
	}()

	return done
}

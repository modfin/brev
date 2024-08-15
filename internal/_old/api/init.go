package api

import (
	"context"
	"fmt"
	"github.com/labstack/echo-contrib/prometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/modfin/brev/dnsx"
	"github.com/modfin/brev/internal/old/config"
	"github.com/modfin/brev/internal/old/dao"
	"github.com/modfin/brev/smtpx/dkim"
	"golang.org/x/crypto/acme/autocert"
	"net/http"
	"strings"
	"sync"
	"time"
)

func Init(ctx context.Context, db dao.DAO, cfg *config.Config) (done chan interface{}) {
	fmt.Println("[API]: Starting API")

	done = make(chan interface{})
	once := sync.Once{}
	closer := func() {
		once.Do(func() {
			close(done)
		})
	}

	signer, err := dkim.NewSigner(cfg.DKIMPrivateKey)
	if err != nil {
		fmt.Println("[API]: Failed to load dkim private key, emails wont be signed using dkim, err;", err)
	}

	e := echo.New()

	prom := prometheus.NewPrometheus("echo", nil)
	e.Use(middleware.Logger(), prom.HandlerFunc)

	e.GET("", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})
	e.POST("/mta", enqueueMTA(db, cfg.DKIMSelector, signer, cfg.Hostname, cfg.MXDomain, dnsx.LookupEmailMX))
	e.PUT("/key-settings", updateKeySettings(db))
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
		var err error
		if cfg.APIAutoTLS {
			email := strings.TrimSpace(cfg.APIAutoTLSEmail)
			if email == "" {
				fmt.Println("[API]: warning BREV_API_AUTO_TLS is enabled, but BREV_API_AUTO_TLS_EMAIL not set")
			}
			e.AutoTLSManager.Cache = autocert.DirCache("/var/lib/brev")
			e.AutoTLSManager.HostPolicy = autocert.HostWhitelist(cfg.Hostname)
			e.AutoTLSManager.Email = email
			e.AutoTLSManager.Listener()
			err = e.StartAutoTLS(fmt.Sprintf(":%d", cfg.APIPort))
		} else {
			err = e.Start(fmt.Sprintf(":%d", cfg.APIPort))
		}
		if err != nil {
			fmt.Println("[API]: stopped err,", err)
		}
		closer()
	}()

	return done
}

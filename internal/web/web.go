package web

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/labstack/gommon/log"
	"github.com/modfin/brev/internal/metrics"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/smtpx/envelope/signer"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/sirupsen/logrus"
	"net/http"
)

type Config struct {
	DefaultHost   string `cli:"default-host"`
	HttpInterface string `cli:"http-interface"`
	HttpPort      int    `cli:"http-port"`

	Signer *signer.Signer
}

func New(ctx context.Context, cfg Config, spool *spool.Spool, metrics *metrics.Metrics, lc *tools.Logger) *Server {

	logger := lc.New("web")

	ss := &Server{
		ctx:     ctx,
		cfg:     cfg,
		log:     logger,
		spool:   spool,
		metrics: metrics,
	}

	ss.starter()

	return ss
}

type Server struct {
	cfg Config
	log *logrus.Logger
	ctx context.Context
	srv *http.Server

	spool   *spool.Spool
	metrics *metrics.Metrics
}

func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) starter() {
	s.log.Infof("Setting up webserver")
	mux := chi.NewRouter()

	mux.Use(s.metrics.Middleware())
	mux.Use(middleware.Recoverer)
	mux.Use(middleware.RequestLogger(&middleware.DefaultLogFormatter{Logger: s.log}))
	s.log.WithField("desc", "health endoint").Infof("/ping")
	mux.Use(middleware.Heartbeat("/ping"))

	s.log.WithField("desc", "endpoint for prometheus metrics").Infof("/metrics")
	mux.Get("/metrics", s.metrics.HttpMetrics())

	s.log.WithField("desc", "http api for sending emails").Infof("/mta")
	mux.Post("/mta", mta(s))

	s.srv = &http.Server{Addr: fmt.Sprintf("%s:%d", s.cfg.HttpInterface, compare.Coalesce(s.cfg.HttpPort, 8080)), Handler: mux}
	go func() {
		s.log.Infof("Starting webserver at %s", fmt.Sprintf("%s:%d", s.cfg.HttpInterface, compare.Coalesce(s.cfg.HttpPort, 8080)))
		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err) // TODO is this what we want?
		}
	}()

}

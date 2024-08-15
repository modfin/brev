package web

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/labstack/gommon/log"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/acme/autocert"
	"net/http"
)

type Config struct {
	Logger *logrus.Logger

	DefaultHost string

	Interface string
	Port      int
}

func New(ctx context.Context, cfg Config) *Server {

	logger := cfg.Logger
	if logger == nil {
		logger = logrus.New()
		logger.AddHook(tools.LoggerWho{Name: "web"})
	}

	return &Server{
		ctx:    ctx,
		config: cfg,
		log:    logger,
	}
}

type Server struct {
	config Config
	log    *logrus.Logger
	ctx    context.Context
	srv    *http.Server

	spool *spool.Spool
}

func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) Start(cfg Config) {

	mux := chi.NewRouter()
	mux.Use(middleware.Recoverer)
	mux.Use(middleware.RequestLogger(&middleware.DefaultLogFormatter{Logger: s.log}))
	mux.Use(middleware.Heartbeat("/ping"))

	mux.Post("/mta", mta(s))

	autocert.NewListener()
	s.srv = &http.Server{Addr: fmt.Sprintf("%s:%d", cfg.Interface, compare.Coalesce(cfg.Port, 8080)), Handler: mux}
	go func() {
		s.log.Info("Starting Webserver")

		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err) // TODO is this what we want?
		}
	}()

}

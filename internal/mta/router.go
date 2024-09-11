package mta

import (
	"context"
	"errors"
	"fmt"
	"github.com/modfin/brev/internal/metrics"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Recoverable errors
var ErrNoAvailableConnections = errors.New("no free available connections to mx server") // probably should not count against retries

var Err4xx = errors.New("a 4xx code, may be gray listing")
var ErrCouldNotConnect = errors.New("could not establish a connection to the server")

var recoverableErrors = []error{
	ErrCouldNotConnect,
	ErrNoAvailableConnections,
	Err4xx,
}

func IsRecoverable(err error) bool {
	return slicez.ContainsBy(recoverableErrors, func(e error) bool {
		return errors.Is(err, e)
	})
}

func newRouter(lc *tools.Logger, config Config, metrics *metrics.Metrics) *router {
	l := lc.New("mta-router")

	concurrency := config.RouterDefaultConcurrency
	if concurrency < 1 {
		l.WithField("concurrency", config.RouterDefaultConcurrency).Warnf("invalid concurrency, replacing with 1")
		concurrency = 1
	}

	l.WithField("concurrency", config.RouterDefaultConcurrency).Infof("creating mta router")

	entries := slicez.Map(config.RouterServerConcurrency, func(srvstring string) (e mapz.Entry[string, int]) {
		srv, strcon, found := strings.Cut(srvstring, "=")
		if !found {
			l.Warnf("missing seperator for server concurrency in %s, seperate with =", srvstring)
			return e
		}
		con, err := strconv.Atoi(strcon)
		if err != nil {
			l.WithError(err).Warnf("could not parse concurrency for server %s, %s is not a int", srv, strcon)
			return e
		}
		return mapz.Entry[string, int]{Key: srv, Value: con}
	})
	entries = slicez.Reject(entries, compare.IsZero[mapz.Entry[string, int]]())

	l.Infof("adding spcific server concurrency for: %v", entries)

	return &router{
		fqdn:               config.FQDN,
		log:                l,
		defaultConcurrency: concurrency,
		servers:            map[string]*server{},
		serverConcurrency:  mapz.FromEntries(entries),
		metrics: routerMetrics{
			borrowHistogram: metrics.Register().NewHistogramVec(prometheus.HistogramOpts{
				Name: "mta_router__borrow_wait_time", Help: "wait time for borrowing a connection to a mx from the router in seconds",
			}, []string{"mx", "success"}),
			sendHistogram: metrics.Register().NewHistogramVec(prometheus.HistogramOpts{
				Name: "mta_router__send_time", Help: "time to send email to mx from the router in seconds",
			}, []string{"mx", "success"}),
		},
	}
}

type routerMetrics struct {
	borrowHistogram *prometheus.HistogramVec
	sendHistogram   *prometheus.HistogramVec
}

type router struct {
	// The "Fully Qualifying Domain Name" is used for "HELO" in smtp, hould be the fully qualified domain name (FQDN) of the sending server
	fqdn               string
	defaultConcurrency int
	serverConcurrency  map[string]int
	servers            map[string]*server
	mu                 sync.Mutex
	log                *logrus.Logger
	metrics            routerMetrics
}

func (r *router) server(name string) *server {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := r.servers[name]
	if s == nil {
		concurrency := r.defaultConcurrency

		host, _, _ := strings.Cut(name, ":")
		concurrency = compare.Coalesce(r.serverConcurrency[host], concurrency)
		if concurrency < 1 {
			concurrency = 1
		}

		s = &server{
			router:      r,
			name:        name,
			concurrency: concurrency, // TODO add specific server concurrency
			conn:        make(chan *connection, concurrency),
		}
		for i := range concurrency { // adding all connections to the server
			s.conn <- &connection{instance: i, s: s}
		}

		r.servers[name] = s
	}

	return s
}

type server struct {
	router      *router
	name        string
	concurrency int
	conn        chan *connection
}

func (s *server) Borrow(ctx context.Context) (*connection, error) {
	select {
	case c := <-s.conn:
		return c, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("context canceled, %w", ErrNoAvailableConnections)
	}
}

type connection struct {
	instance int
	s        *server
	conn     smtpx.Connection
}

func (c *connection) Return() {
	c.s.conn <- c
}

type lgr struct {
	job *spool.Job
}

func (l lgr) Logf(format string, args ...interface{}) error {
	return l.job.Log().Printf(format, args...)
}

func (c *connection) ensureconn(job *spool.Job) error {
	var err error

	lgr := lgr{job: job}
	setlogger := func() {
		if c.conn != nil {
			c.conn.SetLogger(lgr)
		}
	}

	setlogger()
	defer setlogger()

	connect := func() error {
		if c.conn != nil {
			_ = job.Log().With("mx", c.s.name).With("instance", c.instance).Printf("[mta-router] stale connection, forcing close of connection")
			_ = c.conn.Close()
		}

		_ = job.Log().With("mx", c.s.name).With("instance", c.instance).Printf("[mta-router] connecting to server")
		c.conn, err = smtpx.NewConnection(lgr, c.s.name, c.s.router.fqdn, nil)
		if err != nil {
			return fmt.Errorf("could not connect to server: %w, %w", err, ErrCouldNotConnect)
		}
		return nil
	}

	// Check if connection is still alive, if not, reconnect
	if c.conn != nil && c.conn.Noop() == nil {
		return nil
	}
	return connect()
}
func (c *connection) SendMail(job *spool.Job) error {
	_ = job.Log().With("mx", c.s.name).With("instance", c.instance).Printf("[mta-router] sending email")
	c.s.router.log.WithField("mx", c.s.name).WithField("instance", c.instance).Debugf("sending email")

	err := c.ensureconn(job)
	if err != nil {
		return fmt.Errorf("could not ensure connection: %w", err)
	}

	err = c.conn.SendMail(job.From, job.Rcpt, job)

	// Figure out if we should decorate with retry error
	if err != nil {
		err1 := err
		for err1 != nil {
			protoerr, ok := err1.(*textproto.Error)
			// If we get a 4xx error, we should retry
			if ok && 400 <= protoerr.Code && protoerr.Code < 500 {
				err = fmt.Errorf("%w, %w", Err4xx, err)
				break
			}
			err1 = errors.Unwrap(err1)
		}
		err = fmt.Errorf("connection to %s could send email: %w", c.s.name, err)
	}

	return err
}

func (r *router) Route(ctx context.Context, job *spool.Job, servers []string) error {

	if len(servers) == 0 {
		return fmt.Errorf("no servers to send email to")
	}

	r.log.Debugf("fetching server for %s", servers[0])
	srv := r.server(servers[0]) // TODO do something with the other servers if first one fails
	r.log.Debugf("borrowing connection from %s", srv.name)

	startBorrow := time.Now()
	conn, err := srv.Borrow(ctx)
	r.metrics.borrowHistogram.WithLabelValues(srv.name, compare.Ternary(err == nil, "true", "false")).Observe(time.Since(startBorrow).Seconds())
	if err != nil {
		return fmt.Errorf("could not borrow connection: %w", err)
	}
	_ = job.Log().With("mx", srv.name).With("instance", conn.instance).Printf("[mta-router] borrowed connection")

	defer conn.Return()

	startSend := time.Now()
	err = conn.SendMail(job)
	r.metrics.sendHistogram.WithLabelValues(srv.name, compare.Ternary(err == nil, "true", "false")).Observe(time.Since(startSend).Seconds())
	if err != nil {
		return fmt.Errorf("could not send email: %w", err)
	}

	return nil
}

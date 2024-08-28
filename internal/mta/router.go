package mta

import (
	"context"
	"errors"
	"fmt"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
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

func newRouter(lc *tools.Logger, config Config) *router {
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
		log:                l,
		defaultConcurrency: concurrency,
		servers:            map[string]*server{},
		serverConcurrency:  mapz.FromEntries(entries),
	}
}

type router struct {
	defaultConcurrency int
	serverConcurrency  map[string]int
	servers            map[string]*server
	mu                 sync.Mutex
	log                *logrus.Logger
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

func (c *connection) ensureconn(job *spool.Job) error {
	var err error

	setlogger := func() {
		if c.conn != nil {
			c.conn.SetLogger(job)
		}
	}

	setlogger()
	defer setlogger()

	connect := func() error {
		if c.conn != nil {
			job.Logf("[mta-router] forcing close of connection to %s", c.s.name)
			_ = c.conn.Close()
		}

		c.conn, err = smtpx.NewConnection(job, c.s.name, job.LocalName, nil)
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
	fmt.Printf("[conn %s %d] sending mail", c.s.name, c.instance)
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
	srv := r.server(servers[0])

	r.log.Debugf("borrowing connection from %s", srv.name)
	conn, err := srv.Borrow(ctx)
	if err != nil {
		return fmt.Errorf("could not borrow connection: %w", err)
	}
	defer conn.Return()

	err = conn.SendMail(job)
	if err != nil {
		return fmt.Errorf("could not send email: %w", err)
	}
	return nil
}

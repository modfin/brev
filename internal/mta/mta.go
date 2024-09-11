package mta

import (
	"context"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/modfin/brev/internal/dnsx"
	"github.com/modfin/brev/internal/metrics"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Config struct {
	FQDN string `cli:"default-host"`

	PoolSize                 int      `cli:"mta-pool-size"`
	RouterDefaultConcurrency int      `cli:"mta-router-default-concurrency"`
	RouterServerConcurrency  []string `cli:"mta-router-server-concurrency"`

	Wait4Connection time.Duration `cli:"mta-router-max-wait"`

	Retries         int           `cli:"mta-retry"`
	RetriesDuration time.Duration `cli:"mta-retry-duration"`
}

type MTA struct {
	spool queue
	log   *logrus.Logger
	cfg   Config

	ostart sync.Once
	ostop  sync.Once

	pool *pond.WorkerPool
	dns  dnsx.MXer

	router *router

	backoffer *backoffer
	metrics   mtaMetrics
}

type mtaMetrics struct {
	dequeued  prometheus.Counter
	histogram *prometheus.HistogramVec
}

type queue interface {
	Queue() <-chan *spool.Job
}

func New(cfg Config, spool queue, dns dnsx.MXer, metrics *metrics.Metrics, lc *tools.Logger) *MTA {

	logger := lc.New("mta")

	m := &MTA{
		spool:     spool,
		cfg:       cfg,
		log:       logger,
		dns:       dns,
		router:    newRouter(lc, cfg, metrics),
		backoffer: NewBackoffer(compare.Coalesce(cfg.Retries, 5), compare.Coalesce(cfg.RetriesDuration, 24*time.Hour)),
		metrics: mtaMetrics{
			dequeued:  metrics.Register().NewCounter(prometheus.CounterOpts{Name: "mta__dequeued", Help: "number of emails dequeued to be processed by mta"}),
			histogram: metrics.Register().NewHistogramVec(prometheus.HistogramOpts{Name: "mta__duration", Help: "duration borrowHistogram for emails processed"}, []string{"status", "mx"}),
		},
	}

	go m.start()
	return m
}

func (m *MTA) start() {

	m.log.Infof("Starting mta, with %d-%d workers", min(runtime.NumCPU(), m.cfg.PoolSize), m.cfg.PoolSize)
	m.pool = pond.New(m.cfg.PoolSize, 0, pond.MinWorkers(min(runtime.NumCPU(), m.cfg.PoolSize)))

	for job := range m.spool.Queue() {

		m.metrics.dequeued.Inc()

		if m.pool.Stopped() {
			m.log.WithField("eid", job.EID).Warn("pool stopped, skipping email")
			_ = job.Requeue()
			continue
		}

		_ = job.Log().Printf("[mta] submitting job to pool")
		f := m.send(job)
		m.pool.Submit(f)
		m.log.WithField("tid", job.TID).Debug("email submitted to mta")
	}
	m.pool.StopAndWait()
}

func (m *MTA) send(job *spool.Job) func() {

	return func() {
		var label string
		var mx string
		start := time.Now()
		defer func() {
			m.metrics.histogram.WithLabelValues(label, mx).Observe(time.Since(start).Seconds())
		}()
		_ = job.Log().Printf("[mta] starting job")

		mxs, err := m.mxsOf(job.Rcpt)
		if err != nil {
			m.log.WithError(err).WithField("tid", job.TID).Error("could not find mx servers for emails")

			_ = job.Log().
				Error(err).
				Printf("[mta] could not find mx servers for emails %s", job.TID)
			_ = job.Fail()
			label = "failed"
			return
		}
		if len(mxs) > 0 {
			mx = mxs[0]
		}

		_ = job.Log().With("mx", mxs).Printf("[mta] submitting to router with mx servers")
		m.log.WithField("tid", job.TID).Debugf("submitting email to smtp mta router")

		err = m.router.Route(context.Background(), job, mxs)

		if err == nil {
			m.log.WithField("tid", job.TID).Debugf("email has been sent")

			err = job.Success()
			if err != nil {
				_ = job.Log().Error(err).Printf("[mta] could not mark email as sent")
				m.log.WithError(err).WithField("tid", job.TID).Error("could not mark email as successful")

			}
			_ = job.Log().Printf("[mta] done, returning worker to pool")
			label = "success"
			return
		}

		m.log.WithError(err).WithField("tid", job.TID).Error("could not route email")
		_ = job.Log().Error(err).Printf("[mta] could not route email")

		if IsRecoverable(err) && job.Try < m.cfg.Retries {
			m.log.WithField("tid", job.TID).Debugf("email failed but is recoverable, retrying")

			next, err := m.backoffer.Backoff(job.Try)
			if err != nil {
				m.log.WithError(err).WithField("tid", job.TID).Debugf("could not backoff")
				_ = job.Log().Error(err).Printf("[mta] could not backoff")
				_ = job.Fail()
				label = "failed"
				return
			}

			job.NotBefore = time.Now().Add(next)
			job.Try += 1
			err = job.Retry()
			if err != nil {
				m.log.WithError(err).WithField("tid", job.TID).Error("could not add email to retry queue")
				_ = job.Log().Error(err).Printf("[mta] could not add email to retry queue")
			}
			label = "retryable"
			return
		}
		_ = job.Log().Error(err).Printf("[mta] error is not recoverable")
		_ = job.Fail()
		label = "failed"
		return

	}
}

func (m *MTA) Stop(ctx context.Context) error {
	var err error
	m.ostop.Do(func() {

		select {
		case <-m.pool.Stop().Done():
			m.log.Info("mta has been shut down")
		case <-ctx.Done():
			err = ctx.Err()
		}

	})
	return err
}

func (m *MTA) mxsOf(emails []string) ([]string, error) {

	var err error

	emails = slicez.Map(emails, strings.ToLower)

	var domains = slicez.Reject(slicez.Map(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	}), compare.IsZero[string]())

	if len(domains) == 0 {
		return nil, errors.New("no domains found")
	}

	domains = slicez.Uniq(domains)

	if len(domains) > 1 {
		return nil, errors.New("only one domain is allowed")
	}

	domain := domains[0]

	m.log.WithField("domain", domain).Debug("mx lookup for domain")
	mxs, err := m.dns.MX(domain)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("could not find mx for domain %s, err: %w", domain, err))
	}

	if len(mxs) == 0 {
		return nil, errors.New("no mx records found")
	}

	return mxs, err
}

type backoffer struct {
	base     float64
	maxRetry int
}

func NewBackoffer(maxRetries int, totalDuration time.Duration) *backoffer {

	// base * (2^0+2^1 + ... + 2^maxRetries) = totalDuration
	// (2^0+2^1 + ... + 2^maxRetries) = 2^(maxRetries+1) - 1
	geosum := math.Pow(2, float64((maxRetries+1))) - 1
	base := float64(totalDuration) / geosum
	return &backoffer{
		base:     base,
		maxRetry: maxRetries,
	}
}

func (b *backoffer) MustBackoff(retry int) time.Duration {
	return time.Duration(b.base * math.Pow(2, float64(retry)))
}

func (b *backoffer) Backoff(retry int) (time.Duration, error) {
	if retry > b.maxRetry {
		return 0, errors.New("max retries exceeded")
	}
	return time.Duration(b.base * math.Pow(2, float64(retry))), nil
}

package dnsx

import (
	"context"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	"github.com/miekg/dns"
	"github.com/modfin/brev/internal/metrics"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
	"net"
	"strings"
	"time"
)

type client struct {
	mxCache      *ttlcache.Cache[string, []string]
	mu           *tools.KeyedMutex
	log          *logrus.Logger
	resolverHost string
	resolverPort string
}

type Config struct {
	Resolver string `cli:"dns-resolver"`
}

type Client interface {
	MX(domain string) ([]string, error)
	Stop(ctx context.Context) error
}

type MXer interface {
	MX(domain string) ([]string, error)
}

func New(cfg Config, lc *tools.Logger, metrics *metrics.Metrics) Client {
	logger := lc.New("dnsx")
	m := &client{
		mxCache: ttlcache.New[string, []string](ttlcache.WithDisableTouchOnHit[string, []string]()),
		mu:      tools.NewKeyedMutex(),
		log:     logger,
	}

	var err error
	m.resolverHost, m.resolverPort, err = net.SplitHostPort(cfg.Resolver)
	if err != nil {
		m.log.WithError(err).Error("could not split host and port of resolver %s, defaulting to 1.1.1.1 if necessary", cfg.Resolver)
		m.resolverHost = compare.Coalesce(m.resolverHost, "1.1.1.1")
		m.resolverPort = compare.Coalesce(m.resolverPort, "53")
	}

	m.log.Infof("Starting dnsx with resolver %s:%s", m.resolverHost, m.resolverPort)

	go m.mxCache.Start()
	return m
}

func (c *client) Stop(ctx context.Context) error {
	c.mxCache.Stop()
	return nil
}

func (c *client) MX(domain string) ([]string, error) {

	c.mu.Lock(domain)
	defer c.mu.Unlock(domain)

	item := c.mxCache.Get(domain)
	if item != nil {
		return item.Value(), nil
	}

	cli := dns.Client{}
	m := &dns.Msg{}
	m.SetQuestion(dns.Fqdn(domain), dns.TypeMX)
	m.RecursionDesired = true

	//r, _, err := c.Exchange(m, net.JoinHostPort("8.8.8.8", "53"))
	//r, _, err := c.Exchange(m, net.JoinHostPort("127.0.0.1", "53"))
	//r, _, err := c.Exchange(m, net.JoinHostPort("127.0.0.53", "53"))

	r, _, err := cli.Exchange(m, net.JoinHostPort(c.resolverHost, c.resolverPort))
	if err != nil {
		err = fmt.Errorf("could not resolve dns server for domain %s, err: %w", domain, err)
		c.log.WithError(err).WithField("domain", domain).Info("could not resolve dns server")
		return nil, err
	}

	if r.Rcode != dns.RcodeSuccess {
		err = fmt.Errorf("invalid answer name %s after MX query for %s", domain, domain)
		c.log.WithError(err).WithField("dns-rcode", r.Rcode).WithField("domain", domain).Info("invalid answer for domain")
		return nil, err
	}

	mxa := slicez.Map(r.Answer, func(a dns.RR) *dns.MX {
		return a.(*dns.MX)
	})
	mxa = slicez.Reject(mxa, compare.IsZero[*dns.MX]())
	mxa = slicez.SortBy(mxa, func(i, j *dns.MX) bool {
		return i.Preference < j.Preference
	})
	serv := slicez.Map(mxa, func(mx *dns.MX) string {
		return strings.TrimRight(mx.Mx, ".") + ":25"
	})
	ttl := slicez.Min(slicez.Map(mxa, func(mx *dns.MX) uint32 {
		return mx.Hdr.Ttl
	})...)

	if len(serv) == 0 {
		err = fmt.Errorf("no c records for domain %s", domain)
		c.log.WithError(err).WithField("domain", domain).Info("no c records found")
		return nil, err
	}

	c.mxCache.Set(domain, serv, time.Duration(ttl)*time.Second)

	return serv, nil
}

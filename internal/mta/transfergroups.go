package mta

import (
	"errors"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	"github.com/miekg/dns"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"

	"net"
	"strings"
	"time"
)

type Group struct {
	Emails  []string
	Servers []string
}

type mx struct {
	mxCache *ttlcache.Cache[string, []string]
	mu      *tools.KeyedMutex
	log     *logrus.Logger
}

func newMX() *mx {
	logger := logrus.New()
	logger.AddHook(tools.LoggerWho{Name: "mta-mx"})
	m := &mx{
		mxCache: ttlcache.New[string, []string](ttlcache.WithDisableTouchOnHit[string, []string]()),
		mu:      tools.NewKeyedMutex(),
		log:     logger,
	}

	go m.mxCache.Start()
	return m
}

func (mx *mx) mxLookup(domain string) ([]string, error) {

	mx.mu.Lock(domain)
	defer mx.mu.Unlock(domain)

	item := mx.mxCache.Get(domain)
	if item != nil {
		return item.Value(), nil
	}

	c := dns.Client{}
	m := &dns.Msg{}
	m.SetQuestion(dns.Fqdn(domain), dns.TypeMX)
	m.RecursionDesired = true

	//r, _, err := c.Exchange(m, net.JoinHostPort("8.8.8.8", "53"))
	//r, _, err := c.Exchange(m, net.JoinHostPort("127.0.0.1", "53"))
	//r, _, err := c.Exchange(m, net.JoinHostPort("127.0.0.53", "53"))
	r, _, err := c.Exchange(m, net.JoinHostPort("1.1.1.1", "53"))
	if err != nil {
		err = fmt.Errorf("could not resolve dns server for domain %s, err: %w", domain, err)
		mx.log.WithError(err).WithField("domain", domain).Info("could not resolve dns server")
		return nil, err
	}

	if r.Rcode != dns.RcodeSuccess {
		err = fmt.Errorf("invalid answer name %s after MX query for %s", domain, domain)
		mx.log.WithError(err).WithField("dns-rcode", r.Rcode).WithField("domain", domain).Info("invalid answer for domain")
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
		err = fmt.Errorf("no mx records for domain %s", domain)
		mx.log.WithError(err).WithField("domain", domain).Info("no mx records found")
		return nil, err
	}

	mx.mxCache.Set(domain, serv, time.Duration(ttl)*time.Second)

	return serv, nil
}

func (m *mx) GroupEmails(emails []string) ([]*Group, error) {
	emails = slicez.Map(emails, strings.ToLower)

	var buckets = slicez.GroupBy(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	})
	delete(buckets, "")

	var err error

	groups := mapz.Slice(buckets, func(domain string, addresses []string) *Group {
		recs, lerr := m.mxLookup(domain)
		if lerr != nil {
			err = errors.Join(err, fmt.Errorf("could not find mx for domain %s, err: %w", domain, lerr))
			return nil
		}
		return &Group{
			Emails:  addresses,
			Servers: recs,
		}
	})

	groups = slicez.Reject(groups, compare.IsZero[*Group]())
	groups = slicez.Reject(groups, func(a *Group) bool {
		return len(a.Servers) == 0 || len(a.Emails) == 0
	})
	return groups, err
}

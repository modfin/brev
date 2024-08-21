package mta

import (
	"errors"
	"fmt"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"net"
	"strings"
)

type Group struct {
	Emails  []string
	Servers []string
}

func GroupEmails(emails []string) ([]*Group, error) {
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
		recs, lerr := net.LookupMX(domain)
		if lerr != nil {
			err = errors.Join(err, fmt.Errorf("could not find mx for domain %s, err: %w", domain, lerr))
			return nil
		}

		recs = slicez.SortBy(recs, func(i, j *net.MX) bool {
			return i.Pref < j.Pref
		})
		return &Group{
			Emails: addresses,
			Servers: slicez.Map(recs, func(rec *net.MX) string {
				return rec.Host + ":25"
			}),
		}
	})

	groups = slicez.Reject(groups, compare.IsZero[*Group]())
	groups = slicez.Reject(groups, func(a *Group) bool {
		return len(a.Servers) == 0 || len(a.Emails) == 0
	})
	return groups, err
}

package dnsx

import (
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/slicez"
	"net"
	"regexp"
	"strings"
)

type TransferList struct {
	Domain    string
	Emails    []string
	MXServers []string
	Err       error
}

var isIPAddress = regexp.MustCompile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")
var hasPort = regexp.MustCompile("(:)[0-9]+$")

type MXLookup func(emails []string) []TransferList

func LookupEmailMX(emails []string) []TransferList {
	var mx []TransferList

	emails = slicez.Map(emails, strings.ToLower)
	var buckets = slicez.GroupBy(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	})
	delete(buckets, "")

	for domain, addresses := range buckets {

		var err error
		var recs []*net.MX
		if isIPAddress.MatchString(domain) {
			recs = append(recs, &net.MX{
				Host: domain,
			})
		}
		if len(recs) == 0 {
			recs, _ = net.LookupMX(domain) // TODO: error?
		}

		slicez.SortFunc(recs, func(i, j *net.MX) bool {
			return i.Pref < j.Pref
		})
		var servers = slicez.Map(recs, func(rec *net.MX) string {
			if hasPort.MatchString(rec.Host) {
				return rec.Host
			}
			return rec.Host + ":25"
		})

		mx = append(mx, TransferList{
			Domain:    domain,
			Emails:    addresses,
			MXServers: servers,
			Err:       err,
		})

	}
	return mx

}

package dnsx

import (
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/slicez"
	"net"
)

type TransferList struct {
	Domain    string
	Emails    []string
	MXServers []string
	Err       error
}

type MXLookup func(emails []string) []TransferList

func LookupEmailMX(emails []string) []TransferList {
	var mx []TransferList

	var buckets map[string][]string = slicez.GroupBy(emails, func(email string) string {
		domain, err := tools.DomainOfEmail(email)
		if err != nil {
			return ""
		}
		return domain
	})
	delete(buckets, "")

	for domain, addresses := range buckets {
		recs, err := net.LookupMX(domain)
		slicez.SortFunc(recs, func(i, j *net.MX) bool {
			return i.Pref < j.Pref
		})
		var servers []string = slicez.Map(recs, func(rec *net.MX) string {
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

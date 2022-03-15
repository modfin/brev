package dnsx

import (
	"github.com/modfin/brev/tools"
	"net"
	"sort"
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

	var buckets = map[string][]string{}

	for _, address := range emails {
		domain, err := tools.DomainOfEmail(address)
		if err != nil {
			continue
		}
		buckets[domain] = append(buckets[domain], address)
	}

	for domain, addresses := range buckets {
		recs, err := net.LookupMX(domain)
		sort.Slice(recs, func(i, j int) bool {
			return recs[i].Pref < recs[j].Pref
		})
		var servers []string
		for _, rec := range recs {
			servers = append(servers, rec.Host+":25")
		}

		mx = append(mx, TransferList{
			Domain:    domain,
			Emails:    addresses,
			MXServers: servers,
			Err:       err,
		})

	}
	return mx

}

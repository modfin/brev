package dnsx

import (
	"net"
	"net/url"
)

type MX struct {
	Emails []string
	Domain string
	MX     string
	IP     string
}


func LookupEmailMX(emails []string) (map[string]MX, error) {
	var mx = make(map[string]MX)

	for _, to := range emails {
		u, err := url.Parse("smtp://" + to)
		if err != nil {
			return nil, err
		}
		m, ok := mx[u.Host]
		if !ok {
			mx[u.Host] = MX{}
		}
		m = mx[u.Host]
		m.Emails = append(m.Emails, to)
		mx[u.Host] = m

		if m.MX != "" {
			continue
		}

		recs, err := net.LookupMX(u.Host)
		if err != nil {
			return nil, err
		}

		//rand.Shuffle(len(recs), func(i, j int) {
		//	recs[i], recs[j] = recs[j], recs[i]
		//})

		for _, r := range recs {
			ips, err := net.LookupIP(r.Host)
			if err != nil {
				return nil, err
			}

			if len(ips) > 0 {
				m.MX = r.Host
				m.Domain = u.Host
				mx[u.Host] = m
				break
			}
		}
	}
	return mx, nil
}
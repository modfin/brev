package mta

import (
	"fmt"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/smtpx"
)

func newRouter() *router {
	return &router{}
}

type router struct {
}

func (r *router) Route(job *spool.Job, g *group) error {

	if g == nil || len(g.Servers) == 0 {
		return fmt.Errorf("no servers to send email to")
	}

	conn, err := smtpx.NewConnection(job, g.Servers[0], job.LocalName, nil)
	if err != nil {
		return fmt.Errorf("could not connect to server: %w", err)
	}

	err = conn.SendMail(job.From, g.Emails, job)
	if err != nil {
		return fmt.Errorf("could not send email: %w", err)
	}

	return nil
}

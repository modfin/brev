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

func (r *router) Route(job *spool.Job, serv []string) error {

	if len(serv) == 0 {
		return fmt.Errorf("no servers to send email to")
	}

	conn, err := smtpx.NewConnection(job, serv[0], job.LocalName, nil)
	if err != nil {
		return fmt.Errorf("could not connect to server: %w", err)
	}

	err = conn.SendMail(job.From, job.Rcpt, job)

	if err != nil {
		return fmt.Errorf("could not send email: %w", err)
	}

	return nil
}

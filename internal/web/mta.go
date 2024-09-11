package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/pkg/zid"
	"github.com/modfin/brev/smtpx/envelope"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"net/http"
	"strings"
	"time"
)

func respond(w http.ResponseWriter, code int, message string) {
	w.WriteHeader(code)
	w.Write([]byte(message))
}

func mta(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		//ctx := r.Context()
		key := r.URL.Query().Get("key")
		if len(key) > 0 {
			r.Header.Set("Authorization", "Basic "+key)
		}
		kid, secret, ok := r.BasicAuth()
		if !ok {
			respond(w, http.StatusUnauthorized, "no authentication was provided or wrong format")
			return
		}

		fmt.Println(kid, secret)
		// TODO fetch context of user/pass
		// TODO fetch host from api key,
		var host string

		defer r.Body.Close()

		var email = &brev.Email{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(email)
		if err != nil {

			respond(w, http.StatusBadRequest, "could not parse body")
			return
		}

		err = valid(email)
		if err != nil {
			respond(w, http.StatusBadRequest, err.Error())
			return
		}

		if email.Headers == nil {
			email.Headers = brev.Headers{}
		}

		eid := zid.New()
		host = compare.Coalesce(host, s.cfg.DefaultHost)

		escapedFrom := strings.ReplaceAll(email.From.Email, "@", "=")
		// Overwriting Date
		//TODO check if date is already set, and if so, do not overwrite.

		date := email.Headers.GetFirst("Date")
		if !tools.ValidDate(date) {
			email.Headers.Set("Date", []string{time.Now().Format(time.RFC1123Z)})
		}

		messageId := email.Headers.GetFirst("Message-ID") // TODO check domain of message id if provided.
		if !tools.ValidateMessageID(messageId) {
			messageId = fmt.Sprintf("<%s+%s+%s@%s>", eid, escapedFrom, s.cfg.DefaultHost, host)
			email.Headers.Set("Message-ID", []string{messageId})
		}

		emlreader, err := envelope.Marshal(email, s.cfg.Signer)
		if err != nil {
			s.log.WithError(err).Error("could not marshal email")
			respond(w, http.StatusInternalServerError, "could not marshal email")
			return

		}

		recp := email.Recipients()
		groups := slicez.GroupBy(recp, func(s string) string {
			return slicez.Nth(strings.Split(s, "@"), -1)
		})

		var i int
		jobs := slicez.Map(mapz.Values(groups), func(emails []string) spool.Job {
			defer func() { i += 1 }()
			tid := eid.ToTID(i)

			j := s.spool.NewJob()

			j.TID = tid
			j.EID = eid
			j.From = fmt.Sprintf("bounce+%s+%s+%s@%s", tid, escapedFrom, s.cfg.DefaultHost, host)
			j.Rcpt = emails

			_ = j.Log().
				With("kid", kid).
				With("from", email.From.Email).
				With("rcpt", emails).
				WithOmitEmpty("reply-to", email.Headers.GetFirst("Reply-To")).
				Printf("[api] created job")
			return j
		})

		err = s.spool.Enqueue(jobs, emlreader) // todo add signer.
		if err != nil {
			s.log.WithError(err).Error("could not enqueue email")
			respond(w, http.StatusInternalServerError, "could not enqueue email")
			return
		}

		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(jobs)
	}
}

func valid(email *brev.Email) error {
	return errors.Join(
		validateFrom(email),
		validateRecipients(email),
		validateSubject(email),
		validateContent(email),
	)
}

func validateFrom(email *brev.Email) error {
	err := email.From.Valid()
	if err != nil {
		return fmt.Errorf("email %s, is not a valid email address", email.From.String())
	}

	return nil
}

func validateRecipients(email *brev.Email) error {

	for _, a := range slicez.Concat(email.To, email.Cc, email.Bcc) {
		err := a.Valid()
		if err != nil {
			return fmt.Errorf("email %s, is not a valid email address", a.String())
		}
	}
	return nil
}

func validateSubject(email *brev.Email) error {
	if len(email.Subject) == 0 {
		return errors.New("a subject must be provided")
	}
	return nil
}

func validateContent(email *brev.Email) error {
	if len(email.Text) == 0 && len(email.HTML) == 0 {
		return errors.New("content of the email must be provided")
	}
	return nil
}

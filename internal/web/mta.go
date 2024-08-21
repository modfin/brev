package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/internal/spool"
	"github.com/modfin/brev/smtpx/envelope"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"github.com/rs/xid"
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

		// Overwriting Date
		email.Headers.Set("Date", []string{time.Now().In(time.UTC).Format(time.RFC1123Z)})

		eid := xid.New()
		host = compare.Coalesce(host, s.cfg.DefaultHost)
		messageId := fmt.Sprintf("%s@%s", eid.String(), host)
		//Overwriting Message-ID
		email.Headers.Set("Message-ID", []string{messageId})

		email.Metadata.Id = eid
		email.Metadata.ReturnPath = fmt.Sprintf("bounce-%s", messageId)

		if email.Metadata.Conversation && !email.Headers.Has("Reply-To") {
			replyto := brev.Address{
				Name:  email.From.Name,
				Email: fmt.Sprintf("conv-%s-%s@%s", eid.String(), strings.ReplaceAll(email.From.Email, "@", "="), host),
			}
			email.Headers.Set("Reply-To", []string{replyto.String()})
		}

		emlreader, err := envelope.Marshal(email, s.cfg.Signer)
		if err != nil {
			respond(w, http.StatusInternalServerError, "could not marshal email")
			return

		}

		job := spool.Job{
			EID:       email.Metadata.Id,
			MessageId: messageId,
			From:      email.Metadata.ReturnPath,
			Rcpt:      email.Recipients(),
		}
		err = s.spool.Enqueue(job, emlreader) // todo add signer.
		if err != nil {
			respond(w, http.StatusInternalServerError, "could not enqueue email")
			return
		}

		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(job)
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

package envelope

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/envelope/signer"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"io"
)

func Marshal(e *brev.Email, signer *signer.Signer) (io.Reader, error) {
	env, err := From(e)
	if err != nil {
		return nil, fmt.Errorf("could not convert email to envelope, err %v\n", err)
	}

	r := env.Reader()

	if signer != nil {
		pr, pw := io.Pipe()
		go func(r io.Reader) {
			err := signer.Sign(pw, r)
			_ = pw.CloseWithError(err)
		}(r)
		r = pr
	}
	return r, nil
}

func From(email *brev.Email) (*Envelope, error) {
	message := NewEnvelope()

	_ = email.Headers.Delete("return-path")
	message.SetHeaders(email.Headers)

	if email.From == nil {
		return nil, errors.New("email must have a from address")
	}

	message.SetHeader("From", email.From.String())

	to := slicez.Reject(email.To, compare.IsZero[*brev.Address]())
	message.SetHeader("To", slicez.Map(to, func(a *brev.Address) string { return a.String() })...)

	cc := slicez.Reject(email.Cc, compare.IsZero[*brev.Address]())
	message.SetHeader("Cc", slicez.Map(cc, func(a *brev.Address) string { return a.String() })...)

	if len(email.Subject) == 0 {
		return nil, errors.New("email must have a subject")
	}

	if len(email.HTML) == 0 && len(email.Text) == 0 {
		return nil, errors.New("email must have content, html or text must be provided")
	}

	message.SetHeader("Subject", email.Subject)
	if email.HTML != "" && email.Text != "" {
		// order is important, and apparently the text/plain part should go first in the multipart/alternative
		// at least gmail picks the last alternative to display...
		message.SetBody("text/plain", email.Text)
		message.AddAlternative("text/html", email.HTML)
	} else if email.HTML != "" {
		message.SetBody("text/html", email.HTML)
	} else if email.Text != "" {
		message.SetBody("text/plain", email.Text)
	}

	for _, att := range email.Attachments {
		content, err := base64.StdEncoding.DecodeString(att.Content)
		if err != nil {
			return nil, fmt.Errorf("could not base64 decode attachement, err %v\n", err)
		}
		message.Attach(att.Filename, Rename(att.Filename), SetCopyFunc(func(writer io.Writer) error {
			_, err := io.Copy(writer, bytes.NewBuffer(content))
			return err
		}))
	}

	return message, nil
}

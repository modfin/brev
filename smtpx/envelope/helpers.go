package envelope

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/dkim"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"io"
	"net/textproto"
	"strings"
)

func MarshalFromEmail(e *brev.Email, signer *dkim.Signer) ([]byte, error) {
	env, err := From(e)
	if err != nil {
		return nil, err
	}
	b, err := env.Bytes()
	if err != nil {
		return nil, err
	}

	if signer != nil {
		r := textproto.NewReader(bufio.NewReader(bytes.NewReader(b)))
		headers, err := r.ReadMIMEHeader()
		if err != nil {
			return nil, err
		}
		for header, _ := range headers {
			lower := strings.ToLower(header)
			if lower == "return-path" || lower[0] == 'x' { // don't sign unnecessary headers
				continue
			}
			signer = signer.With(dkim.OpAddHeader(strings.ToLower(header)))
		}
		err = signer.Sign(&b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func From(email *brev.Email) (*Envelope, error) {
	message := NewEnvelope()

	_ = email.Headers.Delete("return-path")
	message.SetHeaders(email.Headers)

	message.SetHeader("From", email.From.String())
	to := slicez.Reject(email.To, compare.IsZero[*brev.Address]())
	message.SetHeader("To", slicez.Map(to, func(a *brev.Address) string { return a.String() })...)

	cc := slicez.Reject(email.Cc, compare.IsZero[*brev.Address]())
	message.SetHeader("Cc", slicez.Map(cc, func(a *brev.Address) string { return a.String() })...)

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

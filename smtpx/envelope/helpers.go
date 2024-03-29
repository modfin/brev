package envelope

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/dkim"
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

	for key, values := range email.Headers {
		message.SetHeader(key, values...)
	}
	message.SetHeader("From", message.FormatAddress(email.From.Email, email.From.Name))
	for _, to := range email.To {
		message.AppendHeader("To", message.FormatAddress(to.Email, to.Name))
	}
	for _, cc := range email.Cc {
		message.AppendHeader("Cc", message.FormatAddress(cc.Email, cc.Name))
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

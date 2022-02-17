package brev

import (
	"bytes"
	"encoding/base64"
	"mime"
)

func NewEmail() *Email {
	return &Email{
		Headers: map[string][]string{},
	}
}

type Email struct {
	Headers map[string][]string `json:"headers"`
	From    Address             `json:"from"`
	To      []Address           `json:"to"`
	Cc      []Address           `json:"cc"`
	Bcc     []Address           `json:"bcc"`
	Subject string              `json:"subject"`
	HTML    string              `json:"html"`
	Text    string              `json:"text"`

	Attachments []Attachment `json:"attachments"`
}

func (e *Email) Recipients() []string {
	var recipients []string
	for _, a := range append([]Address(nil), append(append(e.To, e.Cc...), e.Bcc...)...) {
		recipients = append(recipients, a.Email)
	}
	return recipients
}

func (e *Email) AddAttachments(filename string, contentType string, data []byte) {
	e.Attachments = append(e.Attachments, Attachment{
		ContentType: contentType,
		Filename:    filename,
		Content:     base64.StdEncoding.EncodeToString(data),
	})
}

type Address struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func (a Address) String() string {
	if a.Name == "" {
		return a.Email
	}
	hasSpecials := func(text string) bool {
		for i := 0; i < len(text); i++ {
			switch c := text[i]; c {
			case '(', ')', '<', '>', '[', ']', ':', ';', '@', '\\', ',', '.', '"':
				return true
			}
		}
		return false
	}

	var buf = bytes.NewBuffer(nil)
	enc := mime.QEncoding.Encode("UTF-8", a.Name)
	if enc == a.Name {
		buf.WriteByte('"')
		for i := 0; i < len(a.Name); i++ {
			b := a.Name[i]
			if b == '\\' || b == '"' {
				buf.WriteByte('\\')
			}
			buf.WriteByte(b)
		}
		buf.WriteByte('"')
	} else if hasSpecials(a.Name) {
		buf.WriteString(mime.BEncoding.Encode("UTF-8", a.Name))
	} else {
		buf.WriteString(enc)
	}
	buf.WriteString(" <")
	buf.WriteString(a.Email)
	buf.WriteByte('>')
	return buf.String()
}

type Attachment struct {
	ContentType string `json:"content_type"`
	Filename    string `json:"filename"`
	Content     string `json:"content"` // Base64 decoded data...
}

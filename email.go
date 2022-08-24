package brev

import (
	"bytes"
	"encoding/base64"
	"mime"
	"strings"
)

func NewEmail() *Email {
	return &Email{
		Headers: map[string][]string{},
	}
}

type Headers map[string][]string

func (h Headers) keyOf(key string) string {
	cmpKey := strings.ToLower(key)
	for candidate := range h {
		if strings.ToLower(candidate) == cmpKey {
			return candidate
		}
	}
	return key
}
func (h Headers) Has(key string) bool {
	key = h.keyOf(key)
	return len(h[key]) > 0
}

func (h Headers) Set(key string, value []string) {
	key = h.keyOf(key)
	h[key] = value
}
func (h Headers) Add(key string, value string) {
	key = h.keyOf(key)
	h[key] = append(h[key], value)
}
func (h Headers) Get(key string) []string {
	key = h.keyOf(key)
	return h[key]
}
func (h Headers) GetFirst(key string) string {
	key = h.keyOf(key)
	values := h[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type Email struct {
	Headers Headers   `json:"headers"`
	From    Address   `json:"from"`
	To      []Address `json:"to"`
	Cc      []Address `json:"cc"`
	Bcc     []Address `json:"bcc"`
	Subject string    `json:"subject"`
	HTML    string    `json:"html"`
	Text    string    `json:"text"`

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

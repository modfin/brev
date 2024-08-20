package brev

import (
	"encoding/base64"
	"fmt"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/slicez"
	"github.com/rs/xid"
	"net/mail"
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
	value := h[key]
	return len(slicez.Reject(value, compare.IsZero[string]())) > 0
}

func (h Headers) Set(key string, value []string) {
	key = h.keyOf(key)
	h[key] = slicez.Reject(value, compare.IsZero[string]())
}
func (h Headers) Add(key string, value string) {
	key = h.keyOf(key)
	h[key] = slicez.Reject(append(h[key], value), compare.IsZero[string]())
}
func (h Headers) Get(key string) []string {
	key = h.keyOf(key)
	value := h[key]
	return slicez.Reject(value, compare.IsZero[string]())
}
func (h Headers) GetFirst(key string) string {
	key = h.keyOf(key)
	value := h[key]
	value = slicez.Reject(value, compare.IsZero[string]())
	if len(value) == 0 {
		return ""
	}
	return value[0]
}

func (h Headers) Delete(key string) []string {
	key = h.keyOf(key)
	value := slicez.Reject(h[key], compare.IsZero[string]())
	delete(h, key)
	return value

}

type Metadata struct {
	Conversation bool   `json:"conversation"`
	ReturnPath   string `json:"return_path"`
	Id           xid.ID `json:"id"`
}

type Email struct {
	Metadata Metadata

	Headers Headers    `json:"headers"`
	From    *Address   `json:"from"`
	To      []*Address `json:"to"`
	Cc      []*Address `json:"cc"`
	Bcc     []*Address `json:"bcc"`
	Subject string     `json:"subject"`
	HTML    string     `json:"html"`
	Text    string     `json:"text"`

	Attachments []Attachment `json:"attachments"`
}

func (e *Email) Reply() *Email {

	headers := Headers{}

	to := []*Address{e.From}

	if e.Headers.Has("Reply-To") {
		to = slicez.Reject(
			slicez.Map(e.Headers.Get("Reply-To"), func(a string) *Address {
				aa, _ := ParseAddress(a)
				return aa
			}), compare.IsZero[*Address]())
	}
	messageId := e.Headers.GetFirst("Message-ID")
	headers.Set("In-Reply-To", []string{messageId})
	headers.Set("References", e.Headers.Get("References"))
	headers.Add("References", e.Headers.GetFirst("Message-ID"))

	return &Email{
		Metadata: Metadata{
			Conversation: true,
		},
		Headers: headers,
		To:      to,
		Subject: compare.Ternary(strings.HasPrefix(e.Subject, "Re: "), e.Subject, fmt.Sprintf("Re: %s", e.Subject)),
	}
}

func (e *Email) ReplyAll() *Email {
	ee := e.Reply()
	ee.Cc = append(e.To, e.Cc...) // TODO if reply-to header exist, from is ignored.
	if e.Headers.Has("Reply-To") {
		ee.Cc = append(ee.Cc, e.From)
	}
	return ee
}

func (e *Email) Recipients() []string {
	return slicez.Map(slicez.Concat(e.To, e.Cc, e.Bcc), func(a *Address) string {
		return a.Email
	})
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

func (a *Address) Valid() error {
	_, err := ParseAddress(a.String())
	return err
}
func (a *Address) String() string {
	return (&mail.Address{
		Name:    a.Name,
		Address: a.Email,
	}).String()
}

// ParseAddress takes format 'name@example.com', 'First Lastname <name@example.com> and so on
func ParseAddress(address string) (*Address, error) {
	a, err := mail.ParseAddress(address)
	return &Address{
		Name:  a.Name,
		Email: a.Address,
	}, err
}

type Attachment struct {
	ContentType string `json:"content_type"`
	Filename    string `json:"filename"`
	Content     string `json:"content"` // Base64 decoded data...
}

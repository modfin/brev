package brev

import "fmt"

type Email struct {
	Headers map[string]string `json:"headers"`
	From    Address           `json:"from"`
	To      []Address         `json:"to"`
	Cc      []Address         `json:"cc"`
	Bcc     []Address         `json:"bcc"`
	Subject string            `json:"subject"`
	HTML    string            `json:"html"`
	Text    string            `json:"text"`

	Attachments []Attachment `json:"attachments"`
}

func AddressOf(email string) Address {
	return Address{Email: email}
}

type Address struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func (a Address) String() string {
	if len(a.Name) == 0 {
		return a.Email
	}
	return fmt.Sprintf("\"%s\" <%s>", a.Name, a.Email)
}

type Attachment struct {
	ContentType string
	Filename    string
	Content     string // Base64 decoded data...
}

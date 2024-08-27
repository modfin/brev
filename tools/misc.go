package tools

import (
	"errors"
	"fmt"
	"github.com/modfin/henry/slicez"
	"math/rand"
	"net/mail"
	"os"
	"os/user"
	"strings"
)

func SystemUri() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	username := "unknown"
	u, err := user.Current()
	if err == nil {
		username = u.Username
	}
	return fmt.Sprintf("%s@%s", username, hostname), nil
}

func DomainOfEmail(address string) (string, error) {
	parts := strings.Split(address, "@")
	if len(parts) < 2 {
		return "", errors.New("no domain was present in email address")
	}
	return slicez.Nth(parts, -1), nil
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func ValidateMessageID(messageID string) bool {
	if len(messageID) == 0 {
		return false
	}
	// The mail.ParseAddress function parses an email address or a message ID.
	// It returns an error if the input does not conform to the expected format.
	_, err := mail.ParseAddress(messageID)
	return err == nil
}

func ValidDate(date string) bool {
	if date == "" {
		return false
	}
	_, err := mail.ParseDate(date)
	return err == nil
}

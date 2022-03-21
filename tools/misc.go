package tools

import (
	"errors"
	"fmt"
	"math/rand"
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
	return parts[len(parts)-1], nil
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

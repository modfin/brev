package smtpx

import (
	"fmt"
	"github.com/rs/xid"
	"os"
)

func GenerateId() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	return fmt.Sprintf("%s@%s", xid.New().String(), hostname)
}

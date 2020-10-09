package smtpx

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"os"
	"time"
)

func GenerateId() (string, error) {
	random, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return "", nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	pid := os.Getpid()
	nanoTime := time.Now().UTC().UnixNano()

	return fmt.Sprintf("%d.%d.%d@%s", nanoTime, pid, random, hostname), nil
}
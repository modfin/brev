package tools

import (
	"fmt"
	"os"
	"os/user"
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


func Uniq(strs []string) []string {
	set := make(map[string]struct{})

	for _, s := range strs {
		set[s] = struct{}{}
	}
	var res []string
	for s := range set {
		res = append(res, s)
	}
	return res
}
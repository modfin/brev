package brev

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/modfin/henry/slicez"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Client interface {
	Send(ctx context.Context, email *Email) (Receipt, error)
}

type TargetServer struct {
	Host          string
	MxCNAME       string
	isInitialized bool
	downUntil     time.Time
}

type client struct {
	apiKey        string
	domain        string
	posthookUrl   string
	maxRetires    int
	targetServers []TargetServer
	mu            sync.RWMutex
}

// New returns a new brev client that can be used to send emails across multiple brev servers
// apiKey is the key used to gain access to the servers, domain is the domain which the emails will be sent from
// posthookUrl will receive posts with information regarding the status of the sent email
// target servers are the different brev servers that the client will use to send emails
// if we are unable to send an email, maxRetries determines how many extra times we attempt to send the email
func New(apiKey string, domain string, posthookUrl string, targetServers []TargetServer, maxRetries int) (Client, error) {
	apiKey = strings.TrimSpace(apiKey)
	domain = strings.TrimSpace(domain)
	posthookUrl = strings.TrimSpace(posthookUrl)
	if len(apiKey) == 0 {
		return nil, fmt.Errorf("invalid api key")
	}
	if len(domain) == 0 {
		return nil, fmt.Errorf("invalid domain")
	}
	_, err := url.Parse(posthookUrl)
	if err != nil {
		return nil, err
	}
	if len(targetServers) == 0 {
		return nil, fmt.Errorf("at least one target server is required")
	}
	if maxRetries < 0 {
		maxRetries = 0
	}

	for i, _ := range targetServers {
		targetServers[i].Host = strings.TrimRight(targetServers[i].Host, "/")
		targetServers[i].isInitialized = false
		targetServers[i].downUntil = time.Now()
	}
	c := client{
		apiKey:        apiKey,
		domain:        domain,
		posthookUrl:   posthookUrl,
		targetServers: targetServers,
		maxRetires:    maxRetries,
	}
	return &c, nil
}

func (c *client) updateKeySettings(ctx context.Context, targetServer TargetServer) error {
	body, err := json.Marshal(struct {
		Domain      string `json:"domain"`
		MxCNAME     string `json:"mx_cname"`
		PosthookURL string `json:"posthook_url"`
	}{
		Domain:      c.domain,
		MxCNAME:     targetServer.MxCNAME,
		PosthookURL: c.posthookUrl,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", targetServer.Host+"/key-settings?key="+c.apiKey, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Add("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("bad status: %d", resp.StatusCode)
	}
	// update isInitialized since we have successfully updated key settings
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targetServers = slicez.Map(c.targetServers, func(ts TargetServer) TargetServer {
		if ts.Host != targetServer.Host {
			return ts
		}
		ts.isInitialized = true
		return ts
	})
	return nil
}

type Receipt struct {
	EmailId   string   `json:"eid"`
	MessageId string   `json:"message_id"`
	From      string   `json:"from"`
	Recpt     []string `json:"rcpt"`
}

func (c *client) getServer() TargetServer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	availableServers := slicez.Filter(c.targetServers, func(s TargetServer) bool {
		return s.downUntil.Before(time.Now())
	})
	// if no servers are available, any server will be used
	if len(availableServers) == 0 {
		availableServers = c.targetServers
	}
	// return a random server
	return availableServers[rand.Intn(len(availableServers))]
}
func (c *client) handleServerError(targetServer TargetServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	downUntil := time.Now().Add(time.Minute)
	fmt.Printf("[BREV-CLIENT] unable to send emails from host %s flagging server as down until %s\n", targetServer.Host, downUntil.Format(time.RFC3339))
	c.targetServers = slicez.Map(c.targetServers, func(ts TargetServer) TargetServer {
		if ts.Host != targetServer.Host {
			return ts
		}
		ts.downUntil = downUntil
		return ts
	})
}

func (c *client) Send(ctx context.Context, email *Email) (Receipt, error) {
	body, err := json.Marshal(email)
	if err != nil {
		return Receipt{}, err
	}

	var receipt Receipt
	for i := 0; i <= c.maxRetires; i++ {
		select {
		case <-ctx.Done():
			return Receipt{}, ctx.Err()
		default:
		}
		receipt, err = c.send(ctx, body)
		if err == nil {
			return receipt, nil
		}
	}

	return receipt, err
}

func (c *client) send(ctx context.Context, body []byte) (Receipt, error) {
	// decide which server to send to
	server := c.getServer()
	if !server.isInitialized {
		err := c.updateKeySettings(ctx, server)
		if err != nil {
			return Receipt{}, err
		}
	}

	req, err := http.NewRequest("POST", server.Host+"/mta?key="+c.apiKey, bytes.NewBuffer(body))
	if err != nil {
		return Receipt{}, err
	}
	req = req.WithContext(ctx)
	req.Header.Add("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Receipt{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Receipt{}, fmt.Errorf("bad status: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return Receipt{}, err
	}
	var r Receipt
	err = json.Unmarshal(respBytes, &r)
	return r, err
}

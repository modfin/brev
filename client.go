package brev

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func NewClient(apiKey string, host string) *Client {
	host = strings.TrimRight(host, "/")
	return &Client{
		host:   host,
		apiKey: apiKey,
	}
}

type Client struct {
	host   string
	apiKey string
}

type Receipt struct {
	MessageId      string  `json:"message_id"`
	TransactionIds []int64 `json:"transaction_ids"`
}

func (c *Client) UpdateKeySettings(ctx context.Context, domain string, MxCNAME string, posthookUrl url.URL) error {
	body, err := json.Marshal(struct {
		Domain      string `json:"domain"`
		MxCNAME     string `json:"mx_cname"`
		PosthookURL string `json:"posthook_url"`
	}{
		Domain:      domain,
		MxCNAME:     MxCNAME,
		PosthookURL: posthookUrl.String(),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", c.host+"/key-settings?key="+c.apiKey, bytes.NewBuffer(body))
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
	return nil
}

func (c *Client) Send(ctx context.Context, email *Email) (Receipt, error) {

	body, err := json.Marshal(email)
	if err != nil {
		return Receipt{}, err
	}

	req, err := http.NewRequest("POST", c.host+"/mta?key="+c.apiKey, bytes.NewBuffer(body))
	if err != nil {
		return Receipt{}, err
	}
	req = req.WithContext(ctx)
	req.Header.Add("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Receipt{}, err
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

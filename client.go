package brev

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
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
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Receipt{}, err
	}
	var r Receipt
	err = json.Unmarshal(respBytes, &r)
	return r, err
}

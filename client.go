package brev

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
)

func NewClient(apiKey string, url string) *Client {
	url = strings.TrimRight(url, "/")
	return &Client{
		url:    url,
		apiKey: apiKey,
	}
}

type Client struct {
	url    string
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

	req, err := http.NewRequest("POST", c.url+"/mta", bytes.NewBuffer(body))
	if err != nil {
		return Receipt{}, err
	}
	req.WithContext(ctx)
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

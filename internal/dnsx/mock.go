package dnsx

import (
	"context"
)

func NewMock(s string) Client {
	return &MockClient{
		url: s,
	}
}

type MockClient struct {
	url string
}

func (c *MockClient) Stop(ctx context.Context) error {
	return nil
}

func (c *MockClient) MX(domain string) ([]string, error) {
	return []string{c.url}, nil
}

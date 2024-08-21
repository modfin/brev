package signer

import (
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/emersion/go-msgauth/dkim"
	"io"
	"os"
)

type Config struct {
	Domain   string `cli:"dkim-domain"`
	Selector string `cli:"dkim-selector"`

	HeaderCanonicalization string `cli:"dkim-canonicalization-header"`
	BodyCanonicalization   string `cli:"dkim-canonicalization-body"`

	PEMKeyFile string `cli:"dkim-key-file"`
	PEMKey     string `cli:"dkim-key"`
}

type signOptions dkim.SignOptions

func (s *signOptions) WithSigner(signer crypto.Signer) {
	s.Signer = signer
}

func (s *signOptions) WithPEM(pemstr string) error {

	block, _ := pem.Decode([]byte(pemstr))
	if block == nil {
		return fmt.Errorf("could not decode pem, got nil")
	}

	if block.Type != "RSA PRIVATE KEY" {
		return fmt.Errorf("invalid pem type, expected: RSA PRIVATE KEY, got: %s", block.Type)
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("could not parse private key: %w", err)

	}
	s.WithSigner(privateKey)
	return nil
}
func (s *signOptions) WithPEMFile(file string) error {
	pemb, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}
	return s.WithPEM(string(pemb))
}

func (s *signOptions) WithDomain(domain string) {
	s.Domain = domain
}
func (s *signOptions) WithSelector(selector string) {
	s.Selector = selector
}

func (s *signOptions) WithCanonicalization(header, body dkim.Canonicalization) {
	s.HeaderCanonicalization = header
	s.BodyCanonicalization = body
}

type Signer struct {
	options *signOptions
}

type Option func(*signOptions)

func OpDomain(domain string) Option {
	return func(o *signOptions) {
		o.Domain = domain
	}
}
func OpSelector(selector string) Option {
	return func(o *signOptions) {
		o.Domain = selector
	}
}
func From(s *Signer, op ...Option) (*Signer, error) {
	options := &signOptions{
		Domain:                 s.options.Domain,
		Selector:               s.options.Selector,
		Identifier:             s.options.Identifier,
		Signer:                 s.options.Signer,
		Hash:                   s.options.Hash,
		HeaderCanonicalization: s.options.HeaderCanonicalization,
		BodyCanonicalization:   s.options.BodyCanonicalization,
		HeaderKeys:             s.options.HeaderKeys,
		Expiration:             s.options.Expiration,
		QueryMethods:           s.options.QueryMethods,
	}
	for _, o := range op {
		o(options)
	}

	return &Signer{options: options}, nil
}
func New(c Config) (*Signer, error) {

	if c.Domain == "" {
		return nil, fmt.Errorf("dkim: no domain specified")
	}
	if c.Selector == "" {
		return nil, fmt.Errorf("dkim: no selector specified")
	}
	if c.PEMKeyFile == "" && c.PEMKey == "" {
		return nil, fmt.Errorf("dkim: a private key must be specified")
	}

	options := &signOptions{}

	options.WithDomain(c.Domain)
	options.WithSelector(c.Selector)
	options.WithCanonicalization(dkim.Canonicalization(c.HeaderCanonicalization), dkim.Canonicalization(c.BodyCanonicalization))
	var err error
	switch {
	case c.PEMKey != "":
		err = options.WithPEM(c.PEMKey)
	case c.PEMKeyFile != "":
		err = options.WithPEMFile(c.PEMKeyFile)
	}
	if err != nil {
		return nil, fmt.Errorf("could not create signer config: %w", err)
	}

	if options.Signer == nil {
		return nil, fmt.Errorf("dkim: no key was specified in config")
	}

	return &Signer{options: options}, nil
}

func (s *Signer) Sign(out io.Writer, in io.Reader) error {
	return dkim.Sign(out, in, (*dkim.SignOptions)(s.options))
}

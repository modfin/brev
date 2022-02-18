package msa

import (
	"context"
	"fmt"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"github.com/flashmob/go-guerrilla"
	"github.com/flashmob/go-guerrilla/backends"
	"github.com/flashmob/go-guerrilla/mail"
	"regexp"
)

// Mail Submission Agent, aka SMTP server :)
var bounceRegexp = regexp.MustCompile("^bounces_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(=)(.+)(@)(.+)$")

type MSA struct {
	cfg       *guerrilla.AppConfig
	servercfg guerrilla.ServerConfig
	daemon    guerrilla.Daemon
}

func New(ctx context.Context, db dao.DAO, cfg *config.Config) (*MSA, error) {

	fmt.Printf("[MSA]: Starting mail submission agent")

	m := MSA{}

	m.cfg = &guerrilla.AppConfig{}
	m.servercfg = guerrilla.ServerConfig{
		Hostname:        cfg.MXDomain,
		ListenInterface: fmt.Sprintf(":%d", cfg.MXPort),
		IsEnabled:       true,
		//TLS: guerrilla.ServerTLSConfig{
		//	StartTLSOn:     true,
		//	PrivateKeyFile: "/go/src/spool/semper/cert/privkey.pem",
		//	PublicKeyFile:  "/go/src/spool/semper/cert/fullchain.pem",
		//},
	}
	m.cfg.Servers = append(m.cfg.Servers, m.servercfg)
	m.cfg.AllowedHosts = []string{"."} // Wildcard host recip, enforce this in processing instead.
	m.cfg.LogLevel = "info"
	m.daemon = guerrilla.Daemon{Config: m.cfg}
	m.daemon.Backend = &backend{}
	return &m, m.daemon.Start()
}

func handleBounce(transactionIds []string, e *mail.Envelope) backends.Result {
	// TODO implement bounce logic here....
	fmt.Printf("Got bounce for %v\n", transactionIds)
	return backends.NewResult("250 OK: Message received")
}

func handleSendRequest(e *mail.Envelope) backends.Result {
	// TODO convert to brev.Email and pass on to api...
	return backends.NewResult("250 OK: Message received")
}

type ListInterface interface{}

type backend struct{}

// Process processes then saves the mail envelope
func (b *backend) Process(e *mail.Envelope) (result backends.Result) {
	fmt.Println("Processing message:")
	err := e.ParseHeaders()
	if err != nil {
		fmt.Println("could not parse headers,", err)
		return backends.NewResult("500")
	}

	if len(e.RcptTo) < 1 {
		fmt.Println("No RcptTo, ignoring message")
		return backends.NewResult("550")
	}

	if e.Header.Get("X-Brev-Key") != "" {
		return handleSendRequest(e)
	}

	var bounces []string
	for _, r := range e.RcptTo {
		if bounceRegexp.MatchString(r.String()) {
			bounces = append(bounces, r.String())
		}
	}
	if len(bounces) > 0 {
		return handleBounce(bounces, e)
	}

	return backends.NewResult("550 Requested action not taken: mailbox unavailable")
}

func (b *backend) ValidateRcpt(e *mail.Envelope) (err backends.RcptError) {
	return nil
}

func (b *backend) Initialize(backends.BackendConfig) error {
	return nil
}

func (b *backend) Reinitialize() error {
	return nil
}

// Shutdown frees / closes anything created during initializations
func (b *backend) Shutdown() error {
	return nil
}

// Start Starts a backend that has been initialized
func (b *backend) Start() error {
	return nil
}

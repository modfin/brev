package config

import (
	"github.com/caarlos0/env/v6"
	"log"
	"sync"
)

type Config struct {
	MXDomain string `env:"BREV_MX_DOMAIN"` // used to find out what mx servers being used, eg brev.cc
	MXPort   int    `env:"BREV_MX_PORT" envDefault:"25"`

	Hostname string `env:"BREV_HOSTNAME"` // hostname of this particular mx node, eg mx0.brev.cc

	DbURI string `env:"BREV_DB_URI" envDefault:"./brev.sqlite"`

	Workers int `env:"BREV_WORKERS" envDefault:"5"`

	DKIMSelector   string `env:"BREV_DKIM_SELECTOR" envDefault:"brev"` // eg brevcc._domainkey.example.com should contain dkim pub record
	DKIMPrivateKey string `env:"BREV_DKIM_PRIVATE_KEY,file" envDefault:"dkim-private.pem"`

	APIPort         int    `env:"BREV_API_PORT" envDefault:"8080"`
	APIAutoTLS      bool   `env:"BREV_API_AUTO_TLS" envDefault:"false"` // use echo AutoTLSManager for getting a certificate for BREV_HOSTNAME
	APIAutoTLSEmail string `env:"BREV_API_AUTO_TLS_EMAIL"`              // account email for Let's Encrypt

	APIKeys []string `env:"BREV_API_KEYS" envSeparator:","`
}

var (
	once sync.Once
	cfg  Config
)

func Get() *Config {
	once.Do(func() {
		cfg = Config{}
		if err := env.Parse(&cfg); err != nil {
			log.Panic("Couldn't parse Config from env: ", err)
		}
	})
	return &cfg
}

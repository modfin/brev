package config

import (
	"github.com/caarlos0/env/v6"
	"log"
	"sync"
)

type Config struct {
	MXDomain string `env:"BREV_MX_DOMAIN"` // used to find out what mx servers being used, eg brev.cc
	Hostname string `env:"BREV_HOSTNAME"`  // hostname of this particular mx node, eg mx0.brev.cc

	HttpPort int `env:"BREV_HTTP_PORT"`

	SMTPPort int `env:"BREV_SMTP_PORT"`

	DbURI string `env:"BREV_DB_URI" envDefault:"./brev.sqlite"`

	Workers int `env:"BREV_WORKERS" envDefault:"5"`

	DKIMSelector  string `env:"BREV_DKIM_SELECTOR" envDefault:"brev"` // eg brevcc._domainkey.example.com should contain dkim pub record
	DKIMPrivetKey string `env:"BREV_DKIM_PRIVATE_KEY,file" envDefault:"dkim-private.pem"`
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

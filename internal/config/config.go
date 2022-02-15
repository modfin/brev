package config

import (
	"github.com/caarlos0/env/v6"
	"log"
	"sync"
)

type Config struct {
	Hostname string `env:"BREV_HOSTNAME"`

	HttpPort int `env:"BREV_HTTP_PORT"`

	SMTPPort int `env:"BREV_SMTP_PORT"`

	DbURI string `env:"BREV_DB_URI" envDefault:"./brev.sqlite"`

	Workers int `env:"BREV_WORKERS" envDefault:"5"`
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

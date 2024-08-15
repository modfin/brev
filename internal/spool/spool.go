package spool

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/modfin/brev"
	"github.com/modfin/brev/smtpx/dkim"
	"github.com/modfin/brev/smtpx/envelope"
	"os"
	"path/filepath"
)

type Spool struct {
	cfg Config
}

type Config struct {
	Dir string
}

func New(config Config) (*Spool, error) {

	err := os.MkdirAll(filepath.Join(config.Dir, "queue"), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}
	err = os.MkdirAll(filepath.Join(config.Dir, "retry"), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}
	err = os.MkdirAll(filepath.Join(config.Dir, "log"), 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create directory: %w", err)
	}

	return &Spool{cfg: config}, nil
}

type Header struct {
	Id      string   `json:"id"`
	From    string   `json:"from"`
	Rcpt    []string `json:"rcpt"`
	Retries int      `json:"retries"`
}

func (s *Spool) Enqueue(e *brev.Email, signer *dkim.Signer) error {

	h := Header{
		Id:      e.Metadata.Id,
		From:    e.Metadata.ReturnPath,
		Rcpt:    e.Recipients(),
		Retries: 0,
	}

	data, err := envelope.MarshalFromEmail(e, signer)
	if err != nil {
		return fmt.Errorf("could not encode email: %w", err)
	}

	filename := fmt.Sprintf("%s.eml", h.Id)

	head, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("could not encode head: %w", err)
	}

	f, err := os.OpenFile(filepath.Join(s.cfg.Dir, "queue", filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	_, err = f.Write(append(head, '\n'))
	if err != nil {
		return errors.Join(err, f.Close(), os.Remove(f.Name()))
	}
	_, err = f.Write(data)
	if err != nil {
		return errors.Join(err, f.Close(), os.Remove(f.Name()))
	}
	return f.Close()

}

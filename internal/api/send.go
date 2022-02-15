package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/crholm/brev"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"github.com/crholm/brev/tools"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"net/mail"
	"strings"
	"time"
)

func getApiKey(c echo.Context, db dao.DAO) (*dao.ApiKey, error) {
	key := c.QueryParam("key")
	if key == "" {
		return nil, errors.New("an api key must be provided")
	}

	return db.GetApiKey(key)
}

func validateFrom(key *dao.ApiKey, email brev.Email) error {
	// if bad formatting
	from := email.From
	_, err := mail.ParseAddress(from.String())
	if err != nil {
		return fmt.Errorf("email %s, is not a valid email address", from.String())
	}
	domain, err := tools.DomainOfEmail(from.Email)
	if err != nil {
		return err
	}
	if strings.ToLower(domain) != strings.ToLower(key.Domain) {
		return fmt.Errorf("domain %s does not match api key bound domain %s", domain, key.Domain)
	}
	return nil
}

func validateRecipients(email brev.Email) error {

	addresses := append([]brev.Address(nil), email.To...)
	addresses = append(addresses, email.Cc...)
	addresses = append(addresses, email.Bcc...)

	for _, a := range addresses {
		_, err := mail.ParseAddress(a.String())
		if err != nil {
			return fmt.Errorf("email %s, is not a valid email address", a.String())
		}
	}
	return nil
}

func validateSubject(email brev.Email) error {
	if len(email.Subject) == 0 {
		return errors.New("a subject must be provided")
	}
	return nil
}

func validateContent(email brev.Email) error {
	if len(email.Text) == 0 && len(email.HTML) == 0 {
		return errors.New("content of the email must be provided")
	}
	return nil
}

func newMessageId() string {
	return fmt.Sprintf("%s=%s", uuid.New().String(), config.Get().Hostname)
}

func EnqueueMTA(db dao.DAO) echo.HandlerFunc {
	return func(c echo.Context) error {

		key, err := getApiKey(c, db)
		if err != nil {
			return err
		}

		var email brev.Email
		err = c.Bind(&email)
		if err != nil {
			return err
		}

		err = validateFrom(key, email)
		if err != nil {
			return err
		}

		err = validateRecipients(email)
		if err != nil {
			return err
		}

		err = validateSubject(email)
		if err != nil {
			return err
		}

		err = validateContent(email)
		if err != nil {
			return err
		}

		content, err := json.Marshal(email)

		spoolmail := dao.SpoolEmail{
			MessageId:  newMessageId(),
			ApiKey:     key.Key,
			StatusBrev: dao.BrevStatusQueued,
			Content:    content,
			SendAt:     time.Now(),
		}

		err = db.AddEmailToSpool(spoolmail)

		return err
	}
}

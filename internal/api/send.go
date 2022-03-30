package api

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/modfin/brev"
	"github.com/modfin/brev/dnsx"
	"github.com/modfin/brev/internal/dao"
	"github.com/modfin/brev/internal/signals"
	"github.com/modfin/brev/smtpx/dkim"
	"github.com/modfin/brev/smtpx/envelope"
	"github.com/modfin/brev/tools"
	"github.com/modfin/henry/slicez"
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

func validateFrom(key *dao.ApiKey, email *brev.Email) error {
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

func validateRecipients(email *brev.Email) error {

	for _, a := range slicez.Concat(email.To, email.Cc, email.Bcc) {
		_, err := mail.ParseAddress(a.String())
		if err != nil {
			return fmt.Errorf("email %s, is not a valid email address", a.String())
		}
	}
	return nil
}

func validateSubject(email *brev.Email) error {
	if len(email.Subject) == 0 {
		return errors.New("a subject must be provided")
	}
	return nil
}

func validateContent(email *brev.Email) error {
	if len(email.Text) == 0 && len(email.HTML) == 0 {
		return errors.New("content of the email must be provided")
	}
	return nil
}

func newMessageId(hostname string) string {
	return fmt.Sprintf("%s=%s", uuid.New().String(), hostname)
}

func EnqueueMTA(db dao.DAO, dkimSelector string, signer *dkim.Signer, hostname string, defaultMXDomain string, emailMxLookup dnsx.MXLookup) echo.HandlerFunc {

	return func(c echo.Context) error {

		key, err := getApiKey(c, db)
		if err != nil {
			return fmt.Errorf("failed to retrive key, err %v", err)
		}

		email := brev.NewEmail()
		err = c.Bind(&email)
		if err != nil {
			return fmt.Errorf("failed to bind body, err %v", err)
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

		var messageId = newMessageId(hostname)

		// Set default headers
		mxDomain := defaultMXDomain
		if len(key.MxCNAME) > 3 {
			mxDomain = key.MxCNAME
		}
		email.Headers["Message-Id"] = []string{fmt.Sprintf("<%s@%s>", messageId, hostname)}
		if !email.Headers.Has("Date") {
			email.Headers.Add("Date", time.Now().In(time.UTC).Format(envelope.MessageDateFormat))
		}

		returnPath := fmt.Sprintf("bounces_%s@%s", messageId, mxDomain)

		localSigner := signer.
			With(dkim.OpDomain(key.Domain)).
			With(dkim.OpSelector(dkimSelector))

		content, err := envelope.MarshalFromEmail(email, localSigner)
		if err != nil {
			return fmt.Errorf("failed to marshal envelop, err %v", err)
		}

		transferlist := emailMxLookup(email.Recipients())

		var spoolmails []dao.SpoolEmail = slicez.Map(transferlist, func(transfer dnsx.TransferList) (d dao.SpoolEmail) {
			if transfer.Err != nil {
				err = transfer.Err
				fmt.Printf("[API] got error from dns lookup, %v", err)
				return
			}
			return dao.SpoolEmail{
				MessageId:  messageId,
				ApiKey:     key.Key,
				From:       returnPath, // From here is used in the smtp process and the return-path will be added by the receiver
				Recipients: transfer.Emails,
				MXServers:  transfer.MXServers,
			}
		})
		spoolmails = slicez.Reject(spoolmails, func(a dao.SpoolEmail) bool {
			return a.MessageId == ""
		})

		if len(spoolmails) == 0 && err != nil {
			return fmt.Errorf("failed to find recipiants mx servers, err %v", err)
		}

		// Check that context is not canceled before committing to send email.
		select {
		case <-c.Request().Context().Done():
			return c.Request().Context().Err()
		default:

		}

		transactionIds, err := db.EnqueueEmail(spoolmails, content)

		if err != nil {
			return fmt.Errorf("failed to add to spool, err %v", err)
		}

		// Informs MTA to wake up and start processing mail if a sleep at the moment.
		signals.Broadcast(signals.NewMailInSpool)

		return c.JSON(200, brev.Receipt{
			MessageId:      messageId,
			TransactionIds: transactionIds,
		})
	}
}

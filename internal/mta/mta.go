package mta

import (
	"context"
	"errors"
	"fmt"
	"github.com/crholm/brev/internal/config"
	"github.com/crholm/brev/internal/dao"
	"sync"
	"time"
)

// Mail Transfer Agent

func Init(ctx context.Context, db dao.DAO) (done chan interface{}) {
	fmt.Println("Starting MTA")
	done = make(chan interface{})
	once := sync.Once{}
	closer := func() {
		once.Do(func() {
			close(done)
		})
	}

	go func() {
		_ = startMTA(ctx, db)
		closer()
	}()

	return done
}

func startMTA(ctx context.Context, db dao.DAO) error {

	done := make(chan interface{})
	go func() {
		<-ctx.Done()
		fmt.Println("Shutting down mta server")
		close(done)
	}()

	size := config.Get().Workers

	spool := make(chan dao.SpoolEmail, size*2)

	for i := 0; i < size; i++ {
		go worker(spool, db)
	}

	for {

		emails, err := db.GetQueuedEmails(size * 2)

		if err != nil {
			fmt.Println(err)
		}

		if len(emails) > 0 {
			fmt.Println("[MTA]: processing", len(emails), "in internal spool")
		}

		for _, email := range emails {
			err := db.ClaimEmail(email.MessageId)
			if err != nil {
				fmt.Printf("could not claim email %s, %v\n", email.MessageId, err)
				continue
			}
			spool <- email
		}

		// if there is a queue keep working at it.
		if len(emails) > 0 {
			continue
		}

		select {
		// TODO implement wakeup signal
		case <-time.After(10 * time.Second):
		case <-done:
			return errors.New("mta ordered shutdown from context")
		}

	}
}

func worker(spool chan dao.SpoolEmail, db dao.DAO) {

	for email := range spool {

		fmt.Println("[MTA] processing message_id", email.MessageId)
		// TODO claim email
		// TODO convert to email format.
		// TODO sign with dkim
		// TODO extract recipients
		// TODO Lookup recipients MX servers
		// TODO multiplex to over smtp connections.

	}
}

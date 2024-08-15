package hooker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	dao2 "github.com/modfin/brev/internal/old/dao"
	"github.com/modfin/brev/internal/old/signals"
	"github.com/modfin/brev/tools"
	"net/http"
	"sync"
	"time"
)

func New(ctx context.Context, db dao2.DAO) *Posthooker {
	done := make(chan interface{})
	m := &Posthooker{
		done: done,
		ctx:  ctx,
		db:   db,
		closer: func() func() {
			once := sync.Once{}
			return func() {
				once.Do(func() {
					close(done)
				})
			}
		}(),
	}
	return m
}

type Posthooker struct {
	done   chan interface{}
	ctx    context.Context
	db     dao2.DAO
	closer func()
}

func (p *Posthooker) Done() <-chan interface{} {
	return p.done
}

func (p *Posthooker) Stop() {
	p.closer()
}

func (p *Posthooker) Start(workers int) {
	fmt.Println("[Hooker]: Starting Hooker")
	go func() {
		err := p.start(workers)
		if err != nil {
			fmt.Println("[Hooker]: got error from return", err)
		}
		p.closer()
	}()

}

func (p *Posthooker) start(workers int) error {

	localDone := make(chan interface{})
	hookChan := make(chan dao2.Posthook, workers*2)

	go func() {
		select {
		case <-p.ctx.Done():
		case <-p.done:
		}
		close(localDone)
		fmt.Println("[Hooker]: Shutting down hooker server")
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			p.worker(hookChan)
			wg.Done()
		}()
	}

	newHooks, cancel := signals.Listen(signals.NewPosthook)
	defer cancel()
	for {
		hooks, err := p.db.DequeuePosthook(workers * 2)

		if err != nil {
			fmt.Println("[Hooker] err:", err)
		}

		if len(hooks) > 0 {
			fmt.Println("[Hooker]: processing", len(hooks), "posthooks")
		}

		for _, hook := range hooks {
			hookChan <- hook
		}

		// if there is a queue keep working at it.
		if len(hooks) > 0 {
			continue
		}

		select {
		case <-time.After(10 * time.Second):
		case <-newHooks: // Wakeup signal from new hook
		case <-localDone:
			fmt.Println("[Hooker]: Waiting for workers to finish")
			close(hookChan)
			wg.Wait()
			return errors.New("hooker ordered shutdown from context")
		}

	}
}

func (p *Posthooker) worker(hooks <-chan dao2.Posthook) {

	workerId := tools.RandStringRunes(5)

	fmt.Printf("[Hookr-Worker %s]: Starting worker\n", workerId)

	send := func(hook dao2.Posthook) {
		req, err := http.NewRequest("POST", hook.TargetUrl, bytes.NewBuffer([]byte(hook.Content)))
		if err != nil {
			fmt.Printf("[Hookr-Worker %s]: could not create request for target %s, err: %v\n", workerId, hook.TargetUrl, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req.Header.Set("Content-Type", "application/json")
		req.WithContext(ctx)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("[Hookr-Worker %s]: posthook failed for target %s, err %v\n", workerId, hook.TargetUrl, err)
			return
		}
		_ = r.Body.Close()
	}

	for hook := range hooks {
		send(hook)
	}
}

package signals

import (
	"github.com/modfin/henry/slicez"
	"math/rand"
	"sync"
)

type Signal string

const NewMailInSpool Signal = "new-mail-in-spool"
const NewPosthook Signal = "new-posthook"

var mu sync.RWMutex
var sigs = map[Signal][]chan struct{}{}

func Notify(channel Signal) {
	mu.RLock()
	defer mu.RUnlock()
	chans := sigs[channel]
	l := len(chans)
	if l > 0 {
		select {
		case chans[rand.Intn(l)] <- struct{}{}:
		default:
		}
	}
}
func Broadcast(channel Signal) {
	mu.RLock()
	defer mu.RUnlock()
	chans := sigs[channel]
	for _, c := range chans {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

func Listen(channel Signal) (signal <-chan struct{}, cancel func()) {
	mu.Lock()
	defer mu.Unlock()
	c := make(chan struct{}, 1)

	sigs[channel] = append(sigs[channel], c)

	return c, func() {
		mu.Lock()
		defer mu.Unlock()

		sigs[channel] = slicez.Reject(sigs[channel], func(a chan struct{}) bool {
			return a == c
		})
	}
}

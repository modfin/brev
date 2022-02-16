package signals

import (
	"math/rand"
	"sync"
)

type Signal string

const NewMailInSpool Signal = "new-mail-in-spool"

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
		defer mu.Lock()

		var chans []chan struct{}
		for _, cc := range sigs[channel] {
			if cc == c {
				continue
			}
			chans = append(chans, cc)
		}
		sigs[channel] = chans
	}
}

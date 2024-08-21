package tools

import (
	"sync"
)

type KeyedMutex struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

type lockEntry struct {
	mu       sync.Mutex
	refCount int
}

func NewKeyedMutex() *KeyedMutex {
	sl := &KeyedMutex{
		locks: make(map[string]*lockEntry),
	}

	return sl
}

func (km *KeyedMutex) Locked(key string) bool {
	km.mu.Lock()
	defer km.mu.Unlock()
	le, exists := km.locks[key]
	return exists && le.refCount > 0
}

func (km *KeyedMutex) TryLocked(key string) bool {
	km.mu.Lock()
	defer km.mu.Unlock()
	le, exists := km.locks[key]
	if exists && le.refCount > 0 {
		return false
	}
	if !exists {
		le = &lockEntry{}
		km.locks[key] = le
	}
	le.refCount++
	le.mu.Lock()
	return true
}

func (km *KeyedMutex) Lock(key string) {
	km.mu.Lock()
	le, exists := km.locks[key]
	if !exists {
		le = &lockEntry{}
		km.locks[key] = le
	}
	le.refCount++
	km.mu.Unlock()

	le.mu.Lock()
}

func (km *KeyedMutex) Unlock(key string) {
	km.mu.Lock()
	defer km.mu.Unlock()

	le, exists := km.locks[key]
	if !exists {
		panic("unlock of unlocked lock")
	}
	le.refCount--
	if le.refCount == 0 {
		delete(km.locks, key)
	}
	le.mu.Unlock()
}

//func (km *KeyedMutex) cleanup(key string) func() {
//	return func() {
//		km.mu.Lock()
//		defer km.mu.Unlock()
//
//		now := time.Now()
//		le, exists := km.locks[key]
//		if exists && le.refCount == 0 && now.Sub(le.lastUsed) > km.timeout {
//			delete(km.locks, key)
//		}
//	}
//}

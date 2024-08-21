package tools

import (
	"sync"
	"testing"
)

func TestKeyedMutex_LockUnlock(t *testing.T) {
	km := NewKeyedMutex()

	key := "testKey"
	km.Lock(key)
	km.Unlock(key)

	if _, ok := km.locks[key]; ok {
		t.Errorf("Expected mutex for key %s to be removed", key)
	}
}

func TestKeyedMutex_TryLocked(t *testing.T) {
	km := NewKeyedMutex()

	key := "testKey"
	if !km.TryLocked(key) {
		t.Errorf("Expected TryLocked to succeed for key %s", key)
	}

	if km.TryLocked(key) {
		t.Errorf("Expected TryLocked to fail for key %s", key)
	}

	km.Unlock(key)
	if !km.TryLocked(key) {
		t.Errorf("Expected TryLocked to succeed for key %s after unlock", key)
	}
}

func TestKeyedMutex_ConcurrentAccess(t *testing.T) {
	km := NewKeyedMutex()
	key := "testKey"
	var wg sync.WaitGroup

	itr := 1000
	j := 0

	for i := 0; i < itr; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			km.Lock(key)
			j++
			km.Unlock(key)
		}()
	}

	wg.Wait()

	if j != itr {
		t.Errorf("Expected j to be %d, got %d", itr, j)
	}
}

func TestKeyedMutex_Locked(t *testing.T) {
	km := NewKeyedMutex()

	key := "testKey"
	if km.Locked(key) {
		t.Errorf("Expected key %s to be initially unlocked", key)
	}

	km.Lock(key)
	if !km.Locked(key) {
		t.Errorf("Expected key %s to be locked", key)
	}

	km.Unlock(key)
	if km.Locked(key) {
		t.Errorf("Expected key %s to be unlocked after unlock", key)
	}
}

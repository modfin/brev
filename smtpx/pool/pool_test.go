package pool

import (
	"context"
	"errors"
	"fmt"
	"github.com/modfin/brev/smtpx"
	"io"
	"sync"
	"testing"
	"time"
)

type (
	mutexNotify func()
	testMutex   struct {
		mu        sync.Mutex
		hasLocked bool
		isLocked  bool
	}
	testRWMutex struct {
		mu         sync.RWMutex
		hasLocked  bool
		hasRLocked bool
		isLocked   bool
		isRLocked  bool
		onUnlock   mutexNotify
	}
	testConnection struct {
		closed             bool
		closeErr           error
		sendErr            error
		sendMailResultFunc func(from string, to []string, msg io.WriterTo) error
		sendDelay          time.Duration
	}
)

var testErr = errors.New("test error")

func TestPool_cleaner(t *testing.T) {
	type (
		testConnArg struct {
			addr     string
			lastTime time.Duration
			active   bool
			closeErr error
		}
		testCase struct {
			name            string
			conns           []testConnArg
			wantClosedCount int
			wantPoolCount   int
		}
	)

	for _, tc := range []testCase{
		{
			name: "one address, no active connections",
			conns: []testConnArg{
				{addr: "1"},
			},
			wantClosedCount: 0,
			wantPoolCount:   0,
		},
		{
			name: "two active and expired connections, same addr",
			conns: []testConnArg{
				{addr: "1", active: true},
				{addr: "1", active: true},
			},
			wantClosedCount: 2,
			wantPoolCount:   0,
		},
		{
			name: "two active and expired connections, different addr",
			conns: []testConnArg{
				{addr: "1", active: true, lastTime: 5 * time.Minute},
				{addr: "2", active: true, lastTime: 5 * time.Minute},
			},
			wantClosedCount: 2,
			wantPoolCount:   0,
		},
		{
			name: "two active where one is non-expired, same addr",
			conns: []testConnArg{
				{addr: "1", active: true, lastTime: 5 * time.Minute},
				{addr: "1", active: true, lastTime: time.Second},
			},
			wantClosedCount: 1,
			wantPoolCount:   1,
		},
		{
			name: "two active where one is non-expired, different addr",
			conns: []testConnArg{
				{addr: "1", active: true, lastTime: 5 * time.Minute},
				{addr: "2", active: true, lastTime: time.Second},
			},
			wantClosedCount: 1,
			wantPoolCount:   1,
		},
		{
			name: "two active where one is non-expired, different addr",
			conns: []testConnArg{
				{addr: "1", active: true, lastTime: 5 * time.Minute},
				{addr: "2", active: true, lastTime: time.Second},
			},
			wantClosedCount: 1,
			wantPoolCount:   1,
		},
		{
			name: "error while closing conn",
			conns: []testConnArg{
				{addr: "1", active: true, lastTime: 5 * time.Minute, closeErr: testErr},
				{addr: "2", active: false},
			},
			wantClosedCount: 1,
			wantPoolCount:   0,
		},
	} {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				// Run the `pool.cleaner` tests in parallell
				t.Parallel()

				// Set a relatively short context timeout, to cancel cleaner func if something goes wrong during the test
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()

				pool, addLock, lock := newTestPool(nil, 0, time.Millisecond)
				lock.onUnlock = func() {
					// When the cleaner method unlock `lock`, one cleaning cycle have been completed.
					// Cancel context to exit `cleaner` func.
					cancel()
					t.Log("lock.onUnlock: context canceled")
				}

				// Set up connections
				var connectionList []*connection
				var testConnectionList []*testConnection
				for i, tc := range tc.conns {
					conns, ok := pool.connections[tc.addr]
					if !ok {
						conns = &connections{addr: tc.addr}
						pool.connections[tc.addr] = conns
					}
					conn := &connection{id: fmt.Sprintf("testconn_%d", i), mu: &testMutex{}}
					if tc.lastTime != 0 {
						conn.lastMessage = time.Now().Add(-tc.lastTime)
					}
					if tc.active {
						tconn := &testConnection{closeErr: tc.closeErr}
						testConnectionList = append(testConnectionList, tconn)
						conn.conn = tconn
					}
					conns.connections = append(conns.connections, conn)
					connectionList = append(connectionList, conn)
				}

				// Start cleaner func and wait for it to exit
				started := time.Now()
				pool.cleaner(ctx)
				t.Logf("cleaner returned after %v", time.Since(started))

				// Validate that the remaining connections are the expected ones
				if len(pool.connections) != tc.wantPoolCount {
					t.Errorf("ERROR: got %d addresses in pool, want %d", len(pool.connections), tc.wantPoolCount)
				}

				// Check Pool lock status
				validateLockStatus(t, addLock, true, false)
				validateRwLockStatus(t, lock, false, false, true, false)

				// Check connection lock status
				for _, con := range connectionList {
					validateLockStatus(t, con.mu.(*testMutex), true, false)
				}

				// Check number of closed connections
				var gotClosed int
				for _, aConn := range testConnectionList {
					if aConn.closed {
						gotClosed++
					}
				}
				if gotClosed != tc.wantClosedCount {
					t.Errorf("ERROR: got %d closed connections, want %d", gotClosed, tc.wantClosedCount)
				}
			}
		}(tc))
	}
}

func TestPool_SendMail(t *testing.T) {
	type (
		testCase struct {
			name        string
			dialer      smtpx.Dialer
			concurrency int
			wantErrCnt  int
		}
	)

	defaultDialer := func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
		return &testConnection{sendDelay: 10 * time.Millisecond}, nil
	}

	newPool := func(ctx context.Context, tc testCase) (*Pool, *testMutex, *testRWMutex) {
		mu := &testMutex{}
		rwMu := &testRWMutex{}
		dialerFunc := tc.dialer
		if dialerFunc == nil {
			dialerFunc = defaultDialer
		}
		p := New(ctx, dialerFunc, tc.concurrency, "hostname")
		p.lock = rwMu
		p.addLock = mu
		return p, mu, rwMu
	}

	for _, tc := range []testCase{
		{
			name:        "happy_flow",
			concurrency: 4,
		},
		{
			name:        "dialer_error",
			concurrency: 1,
			dialer: func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
				return nil, testErr
			},
			wantErrCnt: 2,
		},
		{
			name:        "send_error",
			concurrency: 1,
			dialer: func(logger smtpx.Logger, addr string, localName string, a smtpx.Auth) (smtpx.Connection, error) {
				return &testConnection{sendDelay: 10 * time.Millisecond, sendErr: testErr}, nil
			},
			wantErrCnt: 2,
		},
	} {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()

				pool, addLock, rwLock := newPool(ctx, tc)

				mts := tc.concurrency * 2 // Mails to send
				respChan := make(chan error)

				// Start parallell sending of mails
				for i := 0; i < mts; i++ {
					go func() {
						respChan <- pool.SendMail(nil, "test", "from", []string{"to"}, nil)
					}()
				}

				var errs []error
				for i := 0; i < mts; i++ {
					err := <-respChan
					if err != nil {
						errs = append(errs, err)
					}
				}
				close(respChan)

				if len(errs) != tc.wantErrCnt {
					t.Errorf("ERROR: got %d errors, want %d\nErrors: %v", len(errs), tc.wantErrCnt, errs)
				}

				validateLockStatus(t, addLock, true, false)
				validateRwLockStatus(t, rwLock, true, false, false, false)
			}
		}(tc))
	}
}

func newTestPool(dialer smtpx.Dialer, concurrency int, cleanPoolInterval time.Duration) (*Pool, *testMutex, *testRWMutex) {
	mu := &testMutex{}
	rwMu := &testRWMutex{}
	return &Pool{
		concurrency:       concurrency,
		dialer:            dialer,
		lock:              rwMu,
		addLock:           mu,
		connections:       map[string]*connections{},
		cleanPoolInterval: cleanPoolInterval,
	}, mu, rwMu
}

func (tc *testConnection) SendMail(from string, to []string, msg io.WriterTo) error {
	if tc.sendDelay > 0 {
		time.Sleep(tc.sendDelay)
	}
	if tc.sendMailResultFunc == nil {
		return tc.sendErr
	}
	return tc.sendMailResultFunc(from, to, msg)
}

func (tc *testConnection) SetLogger(logger smtpx.Logger) {

}

func (tc *testConnection) Close() error {
	tc.closed = true
	return tc.closeErr
}

func validateLockStatus(t *testing.T, mu *testMutex, hasLocked, isLocked bool) {
	if mu.hasLocked != hasLocked {
		t.Errorf("ERROR: got addLock.hasLocked: %v, want: %v", mu.hasLocked, hasLocked)
	}
	if mu.isLocked != isLocked {
		t.Errorf("ERROR: got addLock.isLocked: %v, want: %v", mu.isLocked, isLocked)
	}
}

func validateRwLockStatus(t *testing.T, rw *testRWMutex, hasRead, isRead, hasWrite, isWrite bool) {
	if rw.hasLocked != hasWrite {
		t.Errorf("ERROR: got lock.hasLocked: %v, want: %v", rw.hasLocked, hasWrite)
	}
	if rw.isLocked != isWrite {
		t.Errorf("ERROR: got lock.isLocked: %v, want: %v", rw.isLocked, isWrite)
	}
	if rw.hasRLocked != hasRead {
		t.Errorf("ERROR: got lock.hasRLocked: %v, want: %v", rw.hasRLocked, hasRead)
	}
	if rw.isRLocked != isRead {
		t.Errorf("ERROR: got lock.isRLocked: %v, want: %v", rw.isRLocked, isRead)
	}
}

func (mu *testMutex) Lock() {
	mu.mu.Lock()
	mu.hasLocked = true
	mu.isLocked = true
}

func (mu *testMutex) Unlock() {
	mu.mu.Unlock()
	mu.isLocked = false
}

func (mu *testMutex) reset() {
	mu.hasLocked = false
	mu.isLocked = false
}

func (mu *testRWMutex) Lock() {
	mu.mu.Lock()
	mu.hasLocked = true
	mu.isLocked = true
}

func (mu *testRWMutex) RLock() {
	mu.mu.RLock()
	mu.hasRLocked = true
	mu.isRLocked = true
}

func (mu *testRWMutex) Unlock() {
	mu.mu.Unlock()
	mu.isLocked = false
	if mu.onUnlock != nil {
		mu.onUnlock()
	}
}

func (mu *testRWMutex) RUnlock() {
	mu.mu.RUnlock()
	mu.isRLocked = false
}

func (mu *testRWMutex) reset() {
	mu.hasLocked = false
	mu.hasRLocked = false
	mu.isLocked = false
	mu.isRLocked = false
}

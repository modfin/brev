package pool

import (
	"fmt"
	"github.com/crholm/brev/smtpx"
	"github.com/crholm/brev/tools"
	"io"
	"math/rand"
	"sync"
	"time"
)

func New(dialer smtpx.Dialer, concurrency int) *Pool {
	p := &Pool{
		concurrency: concurrency,
		dialer:      dialer,
		connections: map[string]*connections{},
	}
	go p.cleaner()
	return p
}

type Pool struct {
	concurrency int
	dialer      smtpx.Dialer
	lock        sync.RWMutex
	addLock     sync.Mutex
	connections map[string]*connections
}

func (p *Pool) cleaner() {

	maxLife := time.Minute

	fmt.Printf("[Pool]: starting cleaner\n")
	for {
		select {
		case <-time.After(30 * time.Second):
			// TODO implement close
		}

		now := time.Now()

		p.lock.Lock() // TODO this might be too aggressive of a lock (no one can send emails while active)
		p.addLock.Lock()
		fmt.Printf("[Pool]: starting cleaning of connections...\n")
		start := time.Now()
		for addr, connections := range p.connections {
			var hasOpen bool
			for _, connection := range connections.connections {
				connection.mu.Lock()
				if now.Sub(connection.lastMessage) > maxLife && connection.conn != nil {
					fmt.Printf("[Pool]: removing connection pool for %s\n", addr)
					err := connection.conn.Close()
					if err != nil {
						fmt.Printf("[Pool]: got errer when cloasing connection to %s, err: %v\n", addr, err)
					}
					connection.conn = nil

					connection.mu.Unlock()
					continue
				}
				connection.mu.Unlock()
				hasOpen = true
			}
			if !hasOpen {
				delete(p.connections, addr)
			}

		}
		fmt.Printf("[Pool]: done cleaning of connections, took %v\n", time.Since(start))
		p.addLock.Unlock()
		p.lock.Unlock()
	}
}

func (p *Pool) SendMail(addr string, from string, to []string, msg io.WriterTo) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	conn := p.connections[addr]
	if conn == nil {
		p.lock.RUnlock()
		p.addLock.Lock()
		if p.connections[addr] == nil {
			conn = newConnections(addr, p.dialer, p.concurrency)
			p.connections[addr] = conn
			fmt.Printf("[Pool]: added pool for %s\n", addr)
		}
		p.addLock.Unlock()
		p.lock.RLock()
	}

	return conn.sendMail(from, to, msg)
}

func newConnections(addr string, dialer smtpx.Dialer, concurrency int) *connections {
	c := &connections{
		concurrency: concurrency,
		addr:        addr,
		dialer:      dialer,
	}

	for i := 0; i < concurrency; i++ {
		c.connections = append(c.connections, newConnection(addr, dialer))
	}

	return c
}

type connections struct {
	concurrency int
	addr        string
	dialer      smtpx.Dialer

	connections []*connection
}

func (c *connections) sendMail(from string, to []string, msg io.WriterTo) error {
	con := c.connections[rand.Intn(len(c.connections))] // Avoid locking for roundrobin stuff
	return con.sendMail(from, to, msg)
}

func newConnection(addr string, dialer smtpx.Dialer) *connection {
	return &connection{
		id:     tools.RandStringRunes(8),
		addr:   addr,
		dialer: dialer,
	}
}

type connection struct {
	id          string
	addr        string
	dialer      smtpx.Dialer
	conn        smtpx.Connection
	lastMessage time.Time
	mu          sync.Mutex
}

func (c *connection) connect() error {
	// TODO figure out if im still connected and reconnect if not...
	var err error
	if c.conn == nil {
		start := time.Now()
		c.conn, err = c.dialer(c.addr, nil)
		fmt.Printf("[Pool-conn %s]: connection to %s, took %v\n", c.id, c.addr, time.Since(start))
		if err != nil {
			return err
		}
	}
	return err
}

func (c *connection) sendMail(from string, to []string, msg io.WriterTo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error

	err = c.connect()
	if err != nil {
		fmt.Printf("[Pool-conn %s]: error while connecting to %s, %v\n", c.id, c.addr, err)
		return err
	}

	start := time.Now()
	err = c.conn.SendMail(from, to, msg)
	fmt.Printf("[Pool-conn %s]: Sent email thorugh %s, took %v\n", c.id, c.addr, time.Since(start))
	if err != nil {
		fmt.Printf("[Pool-conn %s]: error while sending email, %v\n", c.id, err)
		return err
	}
	c.lastMessage = time.Now()
	return nil
}

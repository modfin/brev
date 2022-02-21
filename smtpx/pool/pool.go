package pool

import (
	"fmt"
	"github.com/modfin/brev/smtpx"
	"github.com/modfin/brev/tools"
	"io"
	"math/rand"
	"sync"
	"time"
)

func New(dialer smtpx.Dialer, concurrency int, localName string) *Pool {
	p := &Pool{
		localName:   localName,
		concurrency: concurrency,
		dialer:      dialer,
		connections: map[string]*connections{},
	}
	go p.cleaner()
	return p
}

type Pool struct {
	localName   string
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

func (p *Pool) SendMail(logger smtpx.Logger, addr string, from string, to []string, msg io.WriterTo) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	conn := p.connections[addr]
	if conn == nil {
		p.lock.RUnlock()
		p.addLock.Lock()
		if p.connections[addr] == nil {
			conn = newConnections(addr, p.dialer, p.concurrency, p.localName)
			p.connections[addr] = conn
			fmt.Printf("[Pool]: added pool for %s\n", addr)
		}
		p.addLock.Unlock()
		p.lock.RLock()
	}

	return conn.sendMail(logger, from, to, msg)
}

func newConnections(addr string, dialer smtpx.Dialer, concurrency int, localName string) *connections {
	c := &connections{
		localName:   localName,
		concurrency: concurrency,
		addr:        addr,
		dialer:      dialer,
	}

	for i := 0; i < concurrency; i++ {
		c.connections = append(c.connections, newConnection(addr, dialer, localName))
	}

	return c
}

type connections struct {
	localName   string
	concurrency int
	addr        string
	dialer      smtpx.Dialer

	connections []*connection
}

func (c *connections) sendMail(logger smtpx.Logger, from string, to []string, msg io.WriterTo) error {
	con := c.connections[rand.Intn(len(c.connections))] // Avoid locking for roundrobin stuff
	return con.sendMail(logger, from, to, msg)
}

func newConnection(addr string, dialer smtpx.Dialer, localName string) *connection {
	return &connection{
		localName: localName,
		id:        tools.RandStringRunes(8),
		addr:      addr,
		dialer:    dialer,
	}
}

type connection struct {
	id          string
	addr        string
	localName   string
	dialer      smtpx.Dialer
	conn        smtpx.Connection
	lastMessage time.Time
	mu          sync.Mutex
}

func (c *connection) connect(logger smtpx.Logger) error {
	// TODO figure out if im still connected and reconnect if not...
	var err error
	if c.conn == nil {
		start := time.Now()
		c.conn, err = c.dialer(logger, c.addr, c.localName, nil)
		fmt.Printf("[Pool-conn %s]: connection to %s, took %v\n", c.id, c.addr, time.Since(start))
		if err != nil {
			return err
		}
	}
	return err
}

func (c *connection) sendMail(logger smtpx.Logger, from string, to []string, msg io.WriterTo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error

	err = c.connect(logger)
	if err != nil {
		fmt.Printf("[Pool-conn %s]: error while connecting to %s, %v\n", c.id, c.addr, err)
		return err
	}

	start := time.Now()
	err = c.conn.SendMail(logger, from, to, msg)
	fmt.Printf("[Pool-conn %s]: Sent email thorugh %s, took %v\n", c.id, c.addr, time.Since(start))
	if err != nil {
		fmt.Printf("[Pool-conn %s]: error while sending email, %v\n", c.id, err)
		return err
	}
	c.lastMessage = time.Now()
	return nil
}

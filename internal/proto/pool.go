package proto

import (
	"sync"
)

// ConnPool reuses Client connections per address. Each address keeps its own
// idle list; a connection is redialed only after it fails, eliminating the
// dial-per-request cost on the hot cross-node write path.
type ConnPool struct {
	mu     sync.Mutex
	idle   map[string][]*Client
	active map[string]int
	max    int // max concurrent connections per address (0 = unlimited)
}

// NewConnPool returns a pool with the given per-address concurrency cap.
func NewConnPool(maxPerAddr int) *ConnPool {
	return &ConnPool{
		idle:   map[string][]*Client{},
		active: map[string]int{},
		max:    maxPerAddr,
	}
}

// Do borrows (or dials) a client for addr, calls fn, and returns it to the
// pool on success or discards it (and redials later) on failure.
func (p *ConnPool) Do(addr string, fn func(*Client) error) error {
	c, err := p.borrow(addr)
	if err != nil {
		return err
	}
	if err := fn(c); err != nil {
		c.Close()
		p.releaseActive(addr)
		return err
	}
	p.releaseIdle(addr, c)
	return nil
}

func (p *ConnPool) borrow(addr string) (*Client, error) {
	p.mu.Lock()
	if n := len(p.idle[addr]); n > 0 {
		c := p.idle[addr][n-1]
		p.idle[addr] = p.idle[addr][:n-1]
		p.active[addr]++
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	c, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.active[addr]++
	p.mu.Unlock()
	return c, nil
}

func (p *ConnPool) releaseIdle(addr string, c *Client) {
	p.mu.Lock()
	p.active[addr]--
	p.idle[addr] = append(p.idle[addr], c)
	p.mu.Unlock()
}

func (p *ConnPool) releaseActive(addr string) {
	p.mu.Lock()
	p.active[addr]--
	p.mu.Unlock()
}

// Close drains and closes all idle connections.
func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, list := range p.idle {
		for _, c := range list {
			c.Close()
		}
	}
	p.idle = map[string][]*Client{}
}

package proto

import (
	"sync"
)

// ConnPool keeps one gRPC connection per address. A gRPC Client multiplexes
// requests over a single HTTP/2 connection, so one conn per address is enough;
// a conn is closed and redialed only if it fails, eliminating the
// dial-per-request cost on the hot cross-node write path.
type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*Client
	max   int // max concurrent connections per address (kept for compatibility)
}

// NewConnPool returns a pool bounded to maxPerAddr connections per address.
// In practice one connection per address is used since gRPC multiplexes.
func NewConnPool(maxPerAddr int) *ConnPool {
	return &ConnPool{
		conns: map[string]*Client{},
		max:   maxPerAddr,
	}
}

// Do returns the pooled connection for addr and calls fn with it. On a call
// failure the connection is closed (and redialed on the next Do).
func (p *ConnPool) Do(addr string, fn func(*Client) error) error {
	c, err := p.borrow(addr)
	if err != nil {
		return err
	}
	if err := fn(c); err != nil {
		p.drop(addr, c)
		return err
	}
	return nil
}

func (p *ConnPool) borrow(addr string) (*Client, error) {
	p.mu.Lock()
	c := p.conns[addr]
	p.mu.Unlock()
	if c != nil {
		return c, nil
	}
	c, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.conns[addr] = c
	p.mu.Unlock()
	return c, nil
}

// drop closes and removes a broken connection so the next Do redials.
func (p *ConnPool) drop(addr string, c *Client) {
	p.mu.Lock()
	if p.conns[addr] == c {
		delete(p.conns, addr)
	}
	p.mu.Unlock()
	c.Close()
}

// Close closes all pooled connections.
func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.conns {
		c.Close()
		delete(p.conns, addr)
	}
}
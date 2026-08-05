// Package proto defines the newline-delimited JSON wire protocol shared by
// the SQL server and the shard manager's cross-node forwarding.
package proto

import (
	"bufio"
	"encoding/json"
	"net"
)

// Request is a single SQL (or ADMIN) request.
type Request struct {
	ID  int64  `json:"id"`
	SQL string `json:"sql"`
}

// ResultPayload is the outcome of a statement.
type ResultPayload struct {
	Type     string     `json:"type"`
	Columns  []string   `json:"columns,omitempty"`
	Rows     [][]string `json:"rows,omitempty"`
	Affected int64      `json:"affected,omitempty"`
	Note     string     `json:"note,omitempty"` // aux data (e.g. shard advertise addr)
}

// Response is a server reply.
type Response struct {
	ID     int64          `json:"id"`
	OK     bool           `json:"ok"`
	Error  string         `json:"error,omitempty"`
	Result *ResultPayload `json:"result,omitempty"`
}

// Addr is the address a Server listens on.
type Addr interface{ String() string }

// Client talks to a Server over TCP.
type Client struct {
	conn net.Conn
	dec  *json.Decoder
	bw   *bufio.Writer
	id   int64
}

// Dial opens a connection to a server.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		dec:  json.NewDecoder(conn),
		bw:   bufio.NewWriter(conn),
	}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Execute sends one statement and returns the response.
func (c *Client) Execute(q string) (*Response, error) {
	c.id++
	req := &Request{ID: c.id, SQL: q}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := c.bw.Write(append(b, '\n')); err != nil {
		return nil, err
	}
	if err := c.bw.Flush(); err != nil {
		return nil, err
	}
	var resp Response
	if err := c.dec.Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

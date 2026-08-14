// Package proto defines the wire protocol shared by the SQL server and the
// shard manager's cross-node forwarding. Transport is gRPC (HTTP/2); hot
// clients use a persistent bidirectional stream with responses matched by
// request id, so no per-call stream setup is needed.
package proto

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"ydbgo/internal/rpc"
)

// Request is a single SQL (or ADMIN) request.
type Request struct {
	ID  int64
	SQL string
}

// ResultPayload is the outcome of a statement.
type ResultPayload struct {
	Type     string
	Columns  []string
	Rows     [][]string
	Affected int64
	Note     string // aux data (e.g. shard advertise addr)
}

// Response is a server reply.
type Response struct {
	ID     int64
	OK     bool
	Error  string
	Result *ResultPayload
}

// Addr is the address a Server listens on.
type Addr interface{ String() string }

// Client talks to a Server over gRPC, multiplexing all concurrent Execute
// calls over one HTTP/2 bidirectional stream (responses are matched by id).
type Client struct {
	conn grpc.ClientConnInterface
	svc  rpc.YdbServiceClient

	mu      sync.Mutex
	stream  rpc.YdbService_StreamClient
	pending map[int64]chan *rpc.Response
	id      int64
}

// Dial opens a connection to a server.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, svc: rpc.NewYdbServiceClient(conn)}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != nil {
		c.stream.CloseSend()
		c.stream = nil
	}
	c.pending = nil
	if cc, ok := c.conn.(*grpc.ClientConn); ok {
		return cc.Close()
	}
	return nil
}

// Execute sends one statement and returns the response.
func (c *Client) Execute(q string) (*Response, error) {
	c.mu.Lock()
	if err := c.ensureStream(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.id++
	id := c.id
	ch := make(chan *rpc.Response, 1)
	c.pending[id] = ch
	if err := c.stream.Send(&rpc.Request{Id: id, Sql: q}); err != nil {
		delete(c.pending, id)
		c.stream.CloseSend()
		c.stream = nil
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()

	select {
	case resp := <-ch:
		if resp == nil {
			return nil, errors.New("rpc stream closed")
		}
		return ToProtoResponse(resp), nil
	case <-time.After(time.Minute):
		return nil, errors.New("rpc: response timeout")
	}
}

// ensureStream opens the shared stream if none is active and starts the
// receiver goroutine. Callers must hold c.mu.
func (c *Client) ensureStream() error {
	if c.stream != nil {
		return nil
	}
	stream, err := c.svc.Stream(context.Background())
	if err != nil {
		return err
	}
	c.stream = stream
	if c.pending == nil {
		c.pending = map[int64]chan *rpc.Response{}
	}
	go c.readLoop()
	return nil
}

// readLoop drains responses and fans them out to the pending channels.
func (c *Client) readLoop() {
	var stream rpc.YdbService_StreamClient
	c.mu.Lock()
	stream = c.stream
	c.mu.Unlock()
	for {
		resp, err := stream.Recv()
		if err != nil {
			c.failAll(stream)
			return
		}
		c.mu.Lock()
		ch := c.pending[resp.Id]
		delete(c.pending, resp.Id)
		c.mu.Unlock()
		if ch != nil {
			ch <- resp
		}
	}
}

func (c *Client) failAll(s rpc.YdbService_StreamClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream == s {
		c.stream = nil
	}
	for id, ch := range c.pending {
		delete(c.pending, id)
		ch <- nil
	}
}

// ToProtoResponse converts a generated rpc.Response to the internal type.
func ToProtoResponse(r *rpc.Response) *Response {
	out := &Response{ID: r.Id, OK: r.Ok, Error: r.Error}
	if r.Result != nil {
		out.Result = ToProtoPayload(r.Result)
	}
	return out
}

// ToProtoPayload converts a generated rpc.ResultPayload to the internal type.
func ToProtoPayload(p *rpc.ResultPayload) *ResultPayload {
	out := &ResultPayload{Type: p.Type, Columns: p.Columns, Affected: p.Affected, Note: p.Note}
	for _, row := range p.Rows {
		vals := make([]string, len(row.Values))
		for i, v := range row.Values {
			vals[i] = string(v)
		}
		out.Rows = append(out.Rows, vals)
	}
	return out
}

// ToRPCResponse converts the internal response to the generated rpc message.
func ToRPCResponse(r *Response) *rpc.Response {
	out := &rpc.Response{Id: r.ID, Ok: r.OK, Error: r.Error}
	if r.Result != nil {
		out.Result = ToRPCPayload(r.Result)
	}
	return out
}

// ToRPCPayload converts the internal payload to the generated rpc message.
func ToRPCPayload(p *ResultPayload) *rpc.ResultPayload {
	out := &rpc.ResultPayload{Type: p.Type, Columns: p.Columns, Affected: p.Affected, Note: p.Note}
	for _, row := range p.Rows {
		r := &rpc.Row{Values: make([][]byte, len(row))}
		for i, v := range row {
			r.Values[i] = []byte(v)
		}
		out.Rows = append(out.Rows, r)
	}
	return out
}
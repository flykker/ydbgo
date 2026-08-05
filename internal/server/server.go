package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"ydbgo/internal/proto"
	"ydbgo/internal/raftsvc"
	"ydbgo/internal/shard"
	"ydbgo/internal/sql"
)

// Server serves SQL over TCP.
type Server struct {
	ln          net.Listener
	exec        *sql.Executor
	node        *raftsvc.Node
	shards      *shard.Manager
	forwardAddr string // fallback peer SQL address for non-leaders
	mu          sync.Mutex
	closed      bool
}

func NewServer(eng sql.Engine) *Server {
	return &Server{exec: sql.NewExecutor(eng)}
}

// NewClusterServer wraps a Raft node; writes go through consensus.
// forwardAddr is the SQL address of a peer used to forward requests when
// this node is not the leader (the peer chains to the current leader).
func NewClusterServer(n *raftsvc.Node, forwardAddr string) *Server {
	return &Server{node: n, forwardAddr: forwardAddr}
}

// NewShardedServer wraps a shard manager; all routing happens there.
func NewShardedServer(m *shard.Manager) *Server {
	return &Server{shards: m}
}

// Listen starts accepting connections on addr.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return nil
}

func (s *Server) Addr() net.Addr { return s.ln.Addr() }

func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	dec := json.NewDecoder(br)
	for {
		var req proto.Request
		err := dec.Decode(&req)
		if err != nil {
			if err != io.EOF {
				s.writeError(bw, 0, fmt.Sprintf("bad request: %v", err))
				bw.Flush()
			}
			return
		}
		resp := s.handle(&req)
		s.writeJSON(bw, resp)
		bw.Flush()
	}
}

// handleJoin processes "ADMIN JOIN <id> <raftaddr>" on the leader.
func (s *Server) handleJoin(req *proto.Request) *proto.Response {
	if s.node == nil {
		return &proto.Response{ID: req.ID, OK: false, Error: "not a cluster node"}
	}
	if !s.node.IsLeader() {
		if s.forwardAddr != "" {
			return forwardToLeader(req, s.forwardAddr)
		}
		addr := s.node.LeaderAddr()
		if addr != "" {
			return forwardToLeader(req, addr)
		}
		return &proto.Response{ID: req.ID, OK: false, Error: "no leader available"}
	}
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return &proto.Response{ID: req.ID, OK: false, Error: "usage: ADMIN JOIN <node-id> <raft-addr>"}
	}
	id := fields[2]
	raftAddr := fields[3]
	if err := s.node.AddPeer(id, raftAddr); err != nil {
		return &proto.Response{ID: req.ID, OK: false, Error: err.Error()}
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

func (s *Server) handle(req *proto.Request) *proto.Response {
	if req.SQL == "" {
		return &proto.Response{ID: req.ID, OK: false, Error: "empty sql"}
	}
	trimmed := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(trimmed)
	// Sharded mode handles all statements (including ADMIN JOIN, which adds a
	// member to the meta group).
	if s.shards != nil {
		return s.shards.Handle(req)
	}
	// ADMIN JOIN <id> <raftaddr>: add a voter on the leader.
	if strings.HasPrefix(upper, "ADMIN JOIN ") {
		return s.handleJoin(req)
	}
	stmts, err := sql.Parse(req.SQL)
	if err != nil {
		return &proto.Response{ID: req.ID, OK: false, Error: err.Error()}
	}
	if s.node != nil {
		if !s.node.IsLeader() {
			if s.forwardAddr != "" {
				return forwardToLeader(req, s.forwardAddr)
			}
			addr := s.node.LeaderAddr()
			if addr == "" {
				return &proto.Response{ID: req.ID, OK: false, Error: "no leader available"}
			}
			return forwardToLeader(req, addr)
		}
		var last *sql.Result
		r, err := s.node.Execute(req.SQL)
		if err != nil {
			return &proto.Response{ID: req.ID, OK: false, Error: err.Error()}
		}
		if r != nil {
			last = r
		}
		return &proto.Response{ID: req.ID, OK: true, Result: payloadOf(last)}
	}
	var last *sql.Result
	for _, st := range stmts {
		r, err := s.exec.Execute(st)
		if err != nil {
			return &proto.Response{ID: req.ID, OK: false, Error: err.Error()}
		}
		last = r
	}
	return &proto.Response{ID: req.ID, OK: true, Result: payloadOf(last)}
}

// forwardToLeader resends the request to the current leader and relays its
// response back. Used when this node is not the leader.
func forwardToLeader(req *proto.Request, leaderAddr string) *proto.Response {
	var resp *proto.Response
	err := defaultPool.Do(leaderAddr, func(c *proto.Client) error {
		r, err := c.Execute(req.SQL)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	if err != nil {
		return &proto.Response{ID: req.ID, OK: false, Error: err.Error()}
	}
	resp.ID = req.ID
	return resp
}

// defaultPool reuses cross-node connections in the non-sharded forward path.
var defaultPool = proto.NewConnPool(16)

func payloadOf(r *sql.Result) *proto.ResultPayload {
	if r == nil {
		return &proto.ResultPayload{Type: "ok"}
	}
	p := &proto.ResultPayload{Type: r.Type, Affected: r.Affected}
	if len(r.Columns) > 0 {
		p.Columns = r.Columns
		p.Rows = make([][]string, len(r.Rows))
		for i, row := range r.Rows {
			p.Rows[i] = make([]string, len(row))
			for j, v := range row {
				p.Rows[i][j] = v.String()
			}
		}
	}
	return p
}

func (s *Server) writeJSON(w io.Writer, v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	w.Write(append(b, '\n'))
}

func (s *Server) writeError(w io.Writer, id int64, msg string) {
	s.writeJSON(w, &proto.Response{ID: id, OK: false, Error: msg})
}

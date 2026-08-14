package server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"

	"ydbgo/internal/proto"
	"ydbgo/internal/raftsvc"
	"ydbgo/internal/rpc"
	"ydbgo/internal/shard"
	"ydbgo/internal/sql"
)

// Server serves SQL over gRPC.
type Server struct {
	rpc.UnimplementedYdbServiceServer
	gs          *grpc.Server
	ln          net.Listener
	exec        *sql.Executor
	node        *raftsvc.Node
	shards      *shard.Manager
	forwardAddr string // fallback peer SQL address for non-leaders
}

func NewServer(eng sql.Engine) *Server {
	s := &Server{exec: sql.NewExecutor(eng)}
	s.gs = grpc.NewServer()
	rpc.RegisterYdbServiceServer(s.gs, s)
	return s
}

// NewClusterServer wraps a Raft node; writes go through consensus.
// forwardAddr is the SQL address of a peer used to forward requests when
// this node is not the leader (the peer chains to the current leader).
func NewClusterServer(n *raftsvc.Node, forwardAddr string) *Server {
	s := &Server{node: n, forwardAddr: forwardAddr}
	s.gs = grpc.NewServer()
	rpc.RegisterYdbServiceServer(s.gs, s)
	return s
}

// NewShardedServer wraps a shard manager; all routing happens there.
func NewShardedServer(m *shard.Manager) *Server {
	s := &Server{shards: m}
	s.gs = grpc.NewServer()
	rpc.RegisterYdbServiceServer(s.gs, s)
	return s
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
	return s.gs.Serve(s.ln)
}

func (s *Server) Close() error {
	s.gs.Stop()
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// Execute implements rpc.YdbServiceServer (single request/response).
func (s *Server) Execute(_ context.Context, req *rpc.Request) (*rpc.Response, error) {
	return proto.ToRPCResponse(s.handle(&proto.Request{ID: req.Id, SQL: req.Sql})), nil
}

// Stream implements rpc.YdbServiceServer (bidirectional, matched by id).
// Requests are handled by a bounded worker pool so one stream saturates many
// cores without spawning a goroutine per request; Send is serialized because
// a stream admits one writer at a time.
// Stream implements rpc.YdbServiceServer (bidirectional, matched by id).
// Requests are handled concurrently so one stream saturates many cores; Send
// is serialized because a stream admits one writer at a time.
func (s *Server) Stream(stream rpc.YdbService_StreamServer) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(req *rpc.Request) {
			defer wg.Done()
			resp := s.handle(&proto.Request{ID: req.Id, SQL: req.Sql})
			mu.Lock()
			if err := stream.Send(proto.ToRPCResponse(resp)); err != nil {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}(req)
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

func (s *Server) String() string {
	if s.ln != nil {
		return fmt.Sprintf("grpc server on %s", s.ln.Addr())
	}
	return "grpc server (not listening)"
}
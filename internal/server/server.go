package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"ydbgo/internal/proto"
	"ydbgo/internal/raftsvc"
	"ydbgo/internal/rpc"
	"ydbgo/internal/shard"
	"ydbgo/internal/sql"
)

// nodeMetrics tracks request latency/throughput on a standalone server (the
// sharded manager keeps its own copy in internal/shard). Mirrors the JSON
// shape of shard metrics so /api/v1/metrics and /metrics stay uniform.
type nodeMetrics struct {
	mu       sync.Mutex
	writes   int64
	reads    int64
	writHist []time.Duration
	readHist []time.Duration
}

func newNodeMetrics() *nodeMetrics { return &nodeMetrics{} }

func (m *nodeMetrics) record(write bool, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if write {
		m.writes++
		m.writHist = appendHist(m.writHist, d)
	} else {
		m.reads++
		m.readHist = appendHist(m.readHist, d)
	}
}

func (m *nodeMetrics) json() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := func(h []time.Duration, q float64) float64 {
		if len(h) == 0 {
			return 0
		}
		return float64(h[int(q*float64(len(h)))%len(h)]) / 1e6
	}
	rep := map[string]interface{}{
		"writes": m.writes,
		"reads":  m.reads,
		"write_latency_ms": map[string]interface{}{
			"p50": p(m.writHist, 0.5), "p99": p(m.writHist, 0.99), "samples": len(m.writHist),
		},
		"read_latency_ms": map[string]interface{}{
			"p50": p(m.readHist, 0.5), "p99": p(m.readHist, 0.99), "samples": len(m.readHist),
		},
	}
	b, _ := json.Marshal(rep)
	return string(b)
}

// appendHist keeps a bounded reservoir of samples (simple quantile estimate).
func appendHist(h []time.Duration, d time.Duration) []time.Duration {
	if len(h) < 10000 {
		return append(h, d)
	}
	h = h[len(h)/2:]
	return append(h, d)
}

// Server serves SQL over gRPC.
type Server struct {
	rpc.UnimplementedYdbServiceServer
	gs          *grpc.Server
	ln          net.Listener
	exec        *sql.Executor
	node        *raftsvc.Node
	shards      *shard.Manager
	met         *nodeMetrics
	forwardAddr string // fallback peer SQL address for non-leaders
}

func NewServer(eng sql.Engine) *Server {
	s := &Server{exec: sql.NewExecutor(eng), met: newNodeMetrics()}
	s.gs = grpc.NewServer()
	rpc.RegisterYdbServiceServer(s.gs, s)
	return s
}

// NewClusterServer wraps a Raft node; writes go through consensus.
// forwardAddr is the SQL address of a peer used to forward requests when
// this node is not the leader (the peer chains to the current leader).
func NewClusterServer(n *raftsvc.Node, forwardAddr string) *Server {
	s := &Server{node: n, forwardAddr: forwardAddr, met: newNodeMetrics()}
	s.gs = grpc.NewServer()
	rpc.RegisterYdbServiceServer(s.gs, s)
	return s
}

// NewShardedServer wraps a shard manager; all routing happens there.
func NewShardedServer(m *shard.Manager) *Server {
	s := &Server{shards: m, met: newNodeMetrics()}
	s.gs = grpc.NewServer()
	rpc.RegisterYdbServiceServer(s.gs, s)
	return s
}

// StandaloneTables lists catalog summaries for the embedded UI in non-sharded
// mode. The whole table lives on this node as a single shard.
func (s *Server) StandaloneTables() []proto.TableInfo {
	if s.exec == nil {
		return nil
	}
	l, ok := s.exec.Eng.(interface{ SortedTables() []string })
	if !ok {
		return nil
	}
	var out []proto.TableInfo
	for _, name := range l.SortedTables() {
		sch, err := s.exec.Eng.GetSchema(name)
		if err != nil {
			continue
		}
		ti := proto.TableInfo{Name: name, Engine: sch.Engine, Shards: 1}
		for _, c := range sch.Columns {
			primary := c.AsPrimary
			for _, pk := range sch.PK {
				if pk == c.Name {
					primary = true
				}
			}
			ti.Columns = append(ti.Columns, proto.ColumnInfo{Name: c.Name, Type: c.Type.String(), Primary: primary})
		}
		out = append(out, ti)
	}
	return out
}

// StandaloneShards reports the single shard hosting table in non-sharded mode.
func (s *Server) StandaloneShards(table string) ([]proto.ShardInfo, error) {
	if s.exec == nil {
		return nil, fmt.Errorf("cluster view unavailable")
	}
	if _, err := s.exec.Eng.GetSchema(table); err != nil {
		return nil, fmt.Errorf("table %q not found", table)
	}
	return []proto.ShardInfo{{ID: "s0", Nodes: []string{"local"}}}, nil
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

// Handle processes one SQL/ADMIN request directly (shared with the embedded
// UI backend and the gRPC Execute path).
func (s *Server) Handle(req *proto.Request) *proto.Response {
	return s.handle(req)
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
	start := time.Now()
	for _, st := range stmts {
		r, err := s.exec.Execute(st)
		if err != nil {
			return &proto.Response{ID: req.ID, OK: false, Error: err.Error()}
		}
		last = r
	}
	s.met.record(isWrite(stmts), time.Since(start))
	return &proto.Response{ID: req.ID, OK: true, Result: payloadOf(last)}
}

// isWrite reports whether the parsed statements include any write operation.
func isWrite(stmts []sql.Statement) bool {
	for _, st := range stmts {
		switch st.(type) {
		case *sql.CreateTableStmt, *sql.DropTableStmt, *sql.CreateIndexStmt, *sql.DropIndexStmt,
			*sql.InsertStmt, *sql.UpdateStmt, *sql.DeleteStmt, *sql.CreateDatabaseStmt,
			*sql.KVPutStmt, *sql.KVDeleteStmt:
			return true
		}
	}
	return false
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

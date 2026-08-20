package shard

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"ydbgo/internal/proto"
	"ydbgo/internal/raftsvc"
	sqlx "ydbgo/internal/sql"
)

// Config configures a shard Manager on a physical node.
type Config struct {
	ID           string
	SQLAddr      string // this node's SQL server address
	RaftAddr     string // this node's meta-group transport address
	DataDir      string
	RF           int
	ShardSize    uint64 // split threshold in bytes (0 = auto-split disabled)
	SplitTick    time.Duration
	RecoveryTick time.Duration // replica-heal check interval (0 = disabled)
	TTLTick      time.Duration // retention purge check interval (0 = disabled)
}

// ManagedShard is the local replica of one data shard (a Raft group).
type ManagedShard struct {
	spec   *ShardSpec
	node   *raftsvc.Node
	schema *sqlx.TableSchema
	frozen bool // writes rejected while a split migrates rows

	// leader cache: avoids a fresh TCP dial + leader resolution on every
	// routed DML for a shard whose leader is remote.
	ldrMu    sync.Mutex
	ldrAddr  string
	ldrValid time.Time
}

// Manager owns the meta group and all locally-hosted data shards, and routes
// SQL statements to the shards that own the affected key ranges.
type Manager struct {
	mu           sync.RWMutex
	id           string
	sqlAddr      string
	raftAddr     string
	dataDir      string
	rf           int
	shardSize    uint64
	splitTick    time.Duration
	recoveryTick time.Duration
	ttlTick      time.Duration

	meta   *MetaNode
	shards map[string]*ManagedShard

	pool *proto.ConnPool
	met  *metrics

	portMu sync.Mutex
	stop   chan struct{}
	wg     sync.WaitGroup

	// liveness view, maintained by the meta leader's recovery loop
	deadMu    sync.Mutex
	dead      map[string]bool // node IDs currently considered dead
	probeFail map[string]int  // consecutive failed probes per node
}

// NewManager creates a Manager (start via Start).
func NewManager(cfg Config) (*Manager, error) {
	if cfg.ID == "" || cfg.RaftAddr == "" {
		return nil, errors.New("shard manager: id and raft-addr required")
	}
	metaDir := filepath.Join(cfg.DataDir, "meta")
	meta := NewMetaNode(cfg.ID, cfg.RaftAddr, metaDir)
	return &Manager{
		id:           cfg.ID,
		sqlAddr:      cfg.SQLAddr,
		raftAddr:     cfg.RaftAddr,
		dataDir:      cfg.DataDir,
		rf:           cfg.RF,
		shardSize:    cfg.ShardSize,
		splitTick:    cfg.SplitTick,
		recoveryTick: cfg.RecoveryTick,
		ttlTick:      cfg.TTLTick,
		meta:         meta,
		shards:       map[string]*ManagedShard{},
		pool:         proto.NewConnPool(16),
		met:          newMetrics(),
		stop:         make(chan struct{}),
		dead:         map[string]bool{},
		probeFail:    map[string]int{},
	}, nil
}

func hasRaftState(dir string) bool {
	return fileExists(filepath.Join(dir, "raft", "stable"))
}

func fileExists(p string) bool {
	_, err := osStat(p)
	return err == nil
}

// Start boots the meta group, joins/registers the node as configured, then
// remounts any shards this node hosted before a restart.
func (m *Manager) Start(bootstrap bool, joinAddr string) error {
	metaDir := filepath.Join(m.dataDir, "meta")
	already := hasRaftState(metaDir)
	if already {
		bootstrap = false
	}
	if err := m.meta.Start(bootstrap, nil); err != nil {
		return err
	}
	if joinAddr != "" && !already {
		// not yet a member: ask the existing node to add and register us
		if err := m.requestJoin(joinAddr); err != nil {
			return err
		}
		// now wait until we can see the meta leader
		if err := m.waitMetaLeader(10 * time.Second); err != nil {
			return err
		}
	} else {
		// wait until the meta group has a leader
		if err := m.waitMetaLeader(10 * time.Second); err != nil {
			return err
		}
		if bootstrap {
			// single-node bootstrap: register ourselves as the first catalog node
			if err := m.meta.RegisterNode(m.id, m.sqlAddr, m.raftAddr); err != nil {
				return err
			}
		}
	}
	// re-mount locally-hosted shards from the catalog (restart case)
	if err := m.remountShards(); err != nil {
		return err
	}
	if m.splitTick > 0 && m.shardSize > 0 {
		m.wg.Add(1)
		go m.monitorLoop()
	}
	if m.recoveryTick > 0 {
		m.wg.Add(1)
		go m.recoveryLoop()
	}
	if m.ttlTick > 0 {
		m.wg.Add(1)
		go m.ttlLoop()
	}
	return nil
}

// requestJoin asks an existing node to add us to the meta group and register us.
// It retries for a while: a cluster started from config can bring the bootstrap
// node up after the joiners (no ordering guarantee), and joining before the
// target is listening must not kill the node.
func (m *Manager) requestJoin(joinAddr string) error {
	const retryFor = 30 * time.Second
	deadline := time.Now().Add(retryFor)
	var lastErr error
	for {
		lastErr = m.joinOnce(joinAddr)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("join %s after %s: %w", joinAddr, retryFor, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (m *Manager) joinOnce(joinAddr string) error {
	return m.pool.Do(joinAddr, func(c *proto.Client) error {
		resp, err := c.Execute(fmt.Sprintf("ADMIN JOIN %s %s", m.id, m.raftAddr))
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New("join rejected: " + resp.Error)
		}
		// register with the meta leader (join target forwards as needed)
		resp, err = c.Execute(fmt.Sprintf("ADMIN REGISTER %s %s %s", m.id, m.sqlAddr, m.raftAddr))
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New("register rejected: " + resp.Error)
		}
		return nil
	})
}

func (m *Manager) waitMetaLeader(d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if m.meta.IsLeader() || m.meta.LeaderAddr() != "" {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("meta group has no leader")
}

func (m *Manager) remountShards() error {
	cat := m.meta.FSM().Catalog()
	for _, ts := range cat.Tables {
		for _, spec := range ts.Shards {
			if !contains(spec.Nodes, m.id) {
				continue
			}
			isBootstrap := len(spec.Nodes) > 0 && spec.Nodes[0] == m.id
			if _, err := m.mountShard(spec, isBootstrap); err != nil {
				return err
			}
		}
	}
	return nil
}

// Meta returns the meta node (for tests and status).
func (m *Manager) Meta() *MetaNode { return m.meta }

// Close shuts down the manager and all its groups.
func (m *Manager) Close() error {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	m.wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for _, sh := range m.shards {
		if err := sh.node.Close(); err != nil && first == nil {
			first = err
		}
	}
	if err := m.meta.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// Handle processes one request (SQL or ADMIN) on this node.
func (m *Manager) Handle(req *proto.Request) *proto.Response {
	trimmed := strings.TrimSpace(req.SQL)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "ADMIN ") {
		return m.handleAdmin(req, upper)
	}
	stmts, err := sqlx.Parse(trimmed)
	if err != nil {
		return fail(req, err)
	}
	var last *sqlx.Result
	for _, st := range stmts {
		start := time.Now()
		r, err := m.route(st)
		if m.isRead(st) {
			m.met.recordRead(time.Since(start))
			m.met.recordClass(classOf(st), time.Since(start))
		} else {
			m.met.recordWrite(time.Since(start))
		}
		if err != nil {
			return fail(req, err)
		}
		if r != nil {
			last = r
		}
	}
	return &proto.Response{ID: req.ID, OK: true, Result: payloadOf(last)}
}

func (m *Manager) isRead(st sqlx.Statement) bool {
	switch st.(type) {
	case *sqlx.SelectStmt:
		return true
	case *sqlx.KVGetStmt:
		return true
	case *sqlx.KVScanStmt:
		return true
	}
	return false
}

func (m *Manager) route(st sqlx.Statement) (*sqlx.Result, error) {
	switch s := st.(type) {
	case *sqlx.CreateTableStmt:
		return m.ddlCreateTable(s)
	case *sqlx.DropTableStmt:
		return m.ddlDropTable(s)
	case *sqlx.InsertStmt:
		return m.execInsert(s)
	case *sqlx.UpdateStmt:
		return m.execUpdate(s)
	case *sqlx.DeleteStmt:
		return m.execDelete(s)
	case *sqlx.SelectStmt:
		return m.execSelect(s)
	case *sqlx.BeginStmt:
		return &sqlx.Result{Type: "begin"}, nil
	case *sqlx.CommitStmt:
		return &sqlx.Result{Type: "commit"}, nil
	case *sqlx.RollbackStmt:
		return &sqlx.Result{Type: "rollback"}, nil
	case *sqlx.CreateIndexStmt:
		return m.ddlCreateIndex(s)
	case *sqlx.DropIndexStmt:
		return m.ddlDropIndex(s)
	case *sqlx.CreateDatabaseStmt:
		return &sqlx.Result{Type: "create_database"}, nil
	case *sqlx.KVPutStmt:
		return m.execKVPut(s)
	case *sqlx.KVGetStmt:
		return m.execKVGet(s)
	case *sqlx.KVDeleteStmt:
		return m.execKVDelete(s)
	case *sqlx.KVScanStmt:
		return m.execKVScan(s)
	}
	return nil, errors.New("unsupported statement")
}

func fail(req *proto.Request, err error) *proto.Response {
	var id int64
	if req != nil {
		id = req.ID
	}
	return &proto.Response{ID: id, OK: false, Error: err.Error()}
}

func payloadOf(r *sqlx.Result) *proto.ResultPayload {
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

// catalog helpers

func (m *Manager) table(name string) *TableSpec {
	return m.meta.FSM().Catalog().Tables[name]
}

func (m *Manager) nodeSQLAddr(nodeID string) string {
	if n, ok := m.meta.FSM().Catalog().Specs[nodeID]; ok {
		return n.SQLAddr
	}
	return ""
}

// metaLeaderSQLAddr returns the SQL address of the current meta leader.
func (m *Manager) metaLeaderSQLAddr() (string, error) {
	if m.meta.IsLeader() {
		return m.sqlAddr, nil
	}
	leader := m.meta.LeaderID()
	if leader == "" {
		return "", errors.New("meta group has no leader")
	}
	addr := m.nodeSQLAddr(leader)
	if addr == "" {
		return "", fmt.Errorf("meta leader %s not registered", leader)
	}
	return addr, nil
}

// forwardToLeaderSQL resends a request to the meta leader's SQL server.
func (m *Manager) forwardToLeaderSQL(req *proto.Request, sql string) *proto.Response {
	addr, err := m.metaLeaderSQLAddr()
	if err != nil {
		return fail(req, err)
	}
	if addr == m.sqlAddr {
		var id int64
		if req != nil {
			id = req.ID
		}
		return m.Handle(&proto.Request{ID: id, SQL: sql})
	}
	var resp *proto.Response
	err = m.pool.Do(addr, func(c *proto.Client) error {
		r, err := c.Execute(sql)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	if err != nil {
		return fail(req, err)
	}
	if req != nil {
		resp.ID = req.ID
	}
	return resp
}

// ownership

func (m *Manager) owningShard(table string, key string) *ShardSpec {
	ts := m.table(table)
	if ts == nil {
		return nil
	}
	// Ranges may overlap after a split (old shard keeps its whole range while
	// new empty shards are added for hot ranges). Route to the MOST SPECIFIC
	// shard: the one with the largest Start that still contains the key.
	var best *ShardSpec
	for _, sh := range ts.Shards {
		if !sh.Owns(key) {
			continue
		}
		if best == nil || sh.StartAfter(best) {
			best = sh
		}
	}
	return best
}

// hosts reports whether this node hosts the given shard group.
func (m *Manager) hosts(spec *ShardSpec) bool {
	return contains(spec.Nodes, m.id)
}

func (m *Manager) localShard(id string) *ManagedShard {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.shards[id]
}

const shardLeaderCacheTTL = time.Second

// shardLeaderSQLAddr resolves the shard group's leader to its node's SQL
// address. The result is cached for shardLeaderCacheTTL so the hot DML path
// does not dial the network on every statement.
func (m *Manager) shardLeaderSQLAddr(sh *ManagedShard, d time.Duration) (string, error) {
	sh.ldrMu.Lock()
	if sh.ldrAddr != "" && time.Since(sh.ldrValid) < shardLeaderCacheTTL {
		addr := sh.ldrAddr
		sh.ldrMu.Unlock()
		return addr, nil
	}
	sh.ldrMu.Unlock()

	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if sh.node.IsLeader() {
			addr := m.sqlAddr
			sh.ldrMu.Lock()
			sh.ldrAddr, sh.ldrValid = addr, time.Now()
			sh.ldrMu.Unlock()
			return addr, nil
		}
		if id := sh.node.Group().LeaderID(); id != "" {
			if addr := m.nodeSQLAddr(id); addr != "" {
				sh.ldrMu.Lock()
				sh.ldrAddr, sh.ldrValid = addr, time.Now()
				sh.ldrMu.Unlock()
				return addr, nil
			}
		}
		// fallback: during the fresh-join window the bootstrapping placement
		// node (spec.Nodes[0]) is the group leader; during healing that node
		// may be dead, so pick the first reachable placement member instead
		// (any member can forward us to the current leader).
		for _, id := range sh.spec.Nodes {
			if id == m.id {
				continue
			}
			addr := m.nodeSQLAddr(id)
			if addr != "" && nodeReachable(addr) {
				sh.ldrMu.Lock()
				sh.ldrAddr, sh.ldrValid = addr, time.Now()
				sh.ldrMu.Unlock()
				return addr, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("shard %s: leader not resolved to a registered node", sh.spec.ID)
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// scanShard reads all rows from one shard through its leader.
func (m *Manager) scanShard(spec *ShardSpec, sql string) ([]sqlx.Row, error) {
	return m.shardReadRows(spec, sql, nil)
}

// shardReadRows routes a shard-local SELECT through the shard leader so reads
// always observe writes the client already acked: a non-leader replica may lag
// the committed raft log and must not serve stale rows. types, when non-nil,
// carries the expected column types for partial-aggregate result sets whose
// columns do not match the table schema.
func (m *Manager) shardReadRows(spec *ShardSpec, sql string, types []sqlx.Type) ([]sqlx.Row, error) {
	resp, err := m.execShardSQL(spec, sql)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, errors.New(resp.Error)
	}
	if types != nil {
		return rowsFromPayloadTyped(resp.Result, types), nil
	}
	ts := m.table(spec.Table)
	if ts == nil {
		return nil, fmt.Errorf("table %q not found", spec.Table)
	}
	return rowsFromPayload(resp.Result, ts.Schema), nil
}

// scanShardTyped pushes a shard-local SELECT whose result columns do NOT match
// the table schema (partial aggregates / grouped partials). Rows arrive as
// strings over the wire and are reconstructed with the plan's expected types.
func (m *Manager) scanShardTyped(spec *ShardSpec, sql string, types []sqlx.Type) ([]sqlx.Row, error) {
	return m.shardReadRows(spec, sql, types)
}

// scanShardProjected reads only the given columns from one shard so CSTORE
// nodes can run columnar scans, then pads each row back to full schema width.
// tailSQL is an optional " WHERE ..." clause (with any ORDER BY / LIMIT)
// pushed down so the shard-local scan can prune to the query's PK range and
// stop early.
func (m *Manager) scanShardProjected(spec *ShardSpec, cols []string, tailSQL string) ([]sqlx.Row, error) {
	ts := m.table(spec.Table)
	if ts == nil {
		return nil, fmt.Errorf("table %q not found", spec.Table)
	}
	stmt := "SELECT " + strings.Join(cols, ", ") + " FROM " + spec.Table + tailSQL
	rows, err := m.shardReadRows(spec, stmt, nil)
	if err != nil {
		return nil, err
	}
	return expandProjected(cols, ts.Schema, rows), nil
}

// expandProjected reorders/pads projected (narrow) rows back to full schema
// width, filling untouched columns with type zeroes.
func expandProjected(cols []string, schema *sqlx.TableSchema, rows []sqlx.Row) []sqlx.Row {
	colIdx := map[string]int{}
	for i, name := range cols {
		colIdx[name] = i
	}
	out := make([]sqlx.Row, 0, len(rows))
	for _, r := range rows {
		row := make(sqlx.Row, len(schema.Columns))
		for i, c := range schema.Columns {
			ci, ok := colIdx[c.Name]
			if !ok || ci >= len(r) {
				row[i] = zeroForType(c.Type)
				continue
			}
			row[i] = r[ci]
		}
		out = append(out, row)
	}
	return out
}

// mount / unmount

// mountShard creates (or reopens) the local replica of a shard group.
func (m *Manager) mountShard(spec *ShardSpec, isBootstrap bool) (*ManagedShard, error) {
	m.mu.Lock()
	if sh, ok := m.shards[spec.ID]; ok {
		m.mu.Unlock()
		return sh, nil
	}
	m.mu.Unlock()

	dir := filepath.Join(m.dataDir, "shard-"+spec.ID)
	addr, err := m.allocPort(spec.ID)
	if err != nil {
		return nil, err
	}
	schema := m.table(spec.Table)
	if schema == nil {
		return nil, fmt.Errorf("table %q not in catalog", spec.Table)
	}
	node, err := raftsvc.NewNode(raftsvc.Config{ID: m.id, RaftAddr: addr, DataDir: dir})
	if err != nil {
		return nil, err
	}
	// pre-create the table schema so DML applies cleanly (idempotent).
	_ = node.Engine().CreateTable(schema.Schema)

	already := hasRaftState(dir)
	boot := isBootstrap && !already
	if err := node.Start(boot, nil); err != nil {
		node.Close()
		return nil, err
	}
	sh := &ManagedShard{spec: spec, node: node, schema: schema.Schema}
	m.mu.Lock()
	m.shards[spec.ID] = sh
	m.mu.Unlock()
	return sh, nil
}

// waitGroupLeader waits until the group knows its leader (or is the leader).
func waitGroupLeader(g *raftsvc.Group, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if g.IsLeader() || g.LeaderID() != "" {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("group has no leader")
}

// allocPort deterministically picks a transport port for a shard group so it
// stays stable across restarts.
func (m *Manager) allocPort(shardID string) (string, error) {
	m.portMu.Lock()
	defer m.portMu.Unlock()
	host, _, err := net.SplitHostPort(m.raftAddr)
	if err != nil {
		host = "127.0.0.1"
	}
	basePort := portOf(m.raftAddr) + 2000
	port := basePort + int(hashOf(shardID)%500)
	for i := 0; i < 500; i++ {
		addr := net.JoinHostPort(host, strconv.Itoa(port+i))
		l, err := net.Listen("tcp", addr)
		if err == nil {
			l.Close()
			return addr, nil
		}
	}
	return "", errors.New("no free shard port")
}

func portOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 7001
	}
	n, _ := strconv.Atoi(p)
	return n
}

func osStat(p string) (os.FileInfo, error) { return os.Stat(p) }

func osRemoveAll(p string) error { return os.RemoveAll(p) }

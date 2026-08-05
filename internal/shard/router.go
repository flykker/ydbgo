package shard

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"ydbgo/internal/proto"
	sqlx "ydbgo/internal/sql"
	"ydbgo/internal/storage"
)

func notFound(table string) error { return fmt.Errorf("table %q not found", table) }

// handleAdmin dispatches ADMIN commands.
func (m *Manager) handleAdmin(req *proto.Request, upper string) *proto.Response {
	switch {
	case strings.HasPrefix(upper, "ADMIN JOIN "):
		return m.handleAdminJoin(req)
	case strings.HasPrefix(upper, "ADMIN REGISTER "):
		return m.handleRegister(req)
	case strings.HasPrefix(upper, "ADMIN MOUNT-SHARD "):
		return m.handleMount(req)
	case strings.HasPrefix(upper, "ADMIN SHARD-ADD-PEER "):
		return m.handleShardAddPeer(req)
	case strings.HasPrefix(upper, "ADMIN SHARD-REMOVE-PEER "):
		return m.handleShardRemovePeer(req)
	case strings.HasPrefix(upper, "ADMIN SHARD-PEERS "):
		return m.handleShardPeers(req)
	case strings.HasPrefix(upper, "ADMIN SHARD-WAIT-LEADER "):
		return m.handleShardWaitLeader(req)
	case strings.HasPrefix(upper, "ADMIN EXEC-SHARD "):
		return m.handleExecShard(req)
	case strings.HasPrefix(upper, "ADMIN SCAN-SHARD "):
		return m.handleScanShard(req)
	case strings.HasPrefix(upper, "ADMIN FREEZE-SHARD "):
		return m.handleFreezeShard(req, true)
	case strings.HasPrefix(upper, "ADMIN UNFREEZE-SHARD "):
		return m.handleFreezeShard(req, false)
	case strings.HasPrefix(upper, "ADMIN UNMOUNT-SHARD "):
		return m.handleUnmountShard(req)
	case strings.HasPrefix(upper, "ADMIN SHARDS "):
		return m.handleShards(req)
	case strings.HasPrefix(upper, "ADMIN SPLIT "):
		return m.handleSplit(req)
	case strings.HasPrefix(upper, "ADMIN METRICS"):
		return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin", Note: m.met.report()}}
	}
	return fail(req, errors.New("unknown admin command"))
}

// handleAdminJoin adds a node to the meta group. Must run on the meta leader.
func (m *Manager) handleAdminJoin(req *proto.Request) *proto.Response {
	if !m.meta.IsLeader() {
		return m.forwardToLeaderSQL(req, req.SQL)
	}
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return fail(req, errors.New("usage: ADMIN JOIN <node-id> <meta-raft-addr>"))
	}
	if err := m.meta.Group().AddPeer(fields[2], fields[3]); err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleRegister adds a node to the catalog. Must run on the meta leader.
func (m *Manager) handleRegister(req *proto.Request) *proto.Response {
	if !m.meta.IsLeader() {
		return m.forwardToLeaderSQL(req, req.SQL)
	}
	fields := strings.Fields(req.SQL)
	if len(fields) != 5 {
		return fail(req, errors.New("usage: ADMIN REGISTER <node-id> <sql-addr> <raft-addr>"))
	}
	if err := m.meta.RegisterNode(fields[2], fields[3], fields[4]); err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleMount creates/opens the local replica of a shard group. The spec is
// carried in the command (split mounts new shards before the catalog commit).
// Syntax: ADMIN MOUNT-SHARD <table> <shard-id> <start-b64> <end-b64> <bootstrap> <node...>
func (m *Manager) handleMount(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) < 7 {
		return fail(req, errors.New("usage: ADMIN MOUNT-SHARD <table> <shard-id> <start> <end> <bootstrap> <node...>"))
	}
	table, shardID := fields[2], fields[3]
	start, err := decodeKey(fields[4])
	if err != nil {
		return fail(req, err)
	}
	end, err := decodeKey(fields[5])
	if err != nil {
		return fail(req, err)
	}
	boot, _ := strconv.ParseBool(fields[6])
	nodes := fields[7:]
	if !contains(nodes, m.id) {
		return fail(req, fmt.Errorf("node %s does not host shard %s", m.id, shardID))
	}
	// the table may not have replicated to this node yet; wait briefly
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ts := m.table(table); ts != nil {
			break
		}
		if time.Now().After(deadline) {
			return fail(req, notFound(table))
		}
		time.Sleep(20 * time.Millisecond)
	}
	spec := &ShardSpec{ID: shardID, Table: table, Start: start, End: end, Nodes: nodes}
	sh, err := m.mountShard(spec, boot)
	if err != nil {
		return fail(req, err)
	}
	adv := sh.node.Group().Advertise()
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin", Note: adv}}
}

// handleShardAddPeer adds a voter to a shard group (any placement node).
func (m *Manager) handleShardAddPeer(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 6 {
		return fail(req, errors.New("usage: ADMIN SHARD-ADD-PEER <table> <shard-id> <peer-id> <peer-raft-addr>"))
	}
	table, shardID, peerID, peerAddr := fields[2], fields[3], fields[4], fields[5]
	_ = table
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	if !sh.node.IsLeader() {
		addr, err := m.shardLeaderSQLAddr(sh, 5*time.Second)
		if err != nil {
			return fail(req, err)
		}
		if addr != m.sqlAddr {
			resp, err := m.remoteAdmin(addr, req.SQL)
			if err != nil {
				return fail(req, err)
			}
			resp.ID = req.ID
			return resp
		}
		// we are the leader but the election just finished; fall through
	}
	if err := sh.node.AddPeer(peerID, peerAddr); err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleShardRemovePeer removes a voter from a shard group (any placement node).
// Syntax: ADMIN SHARD-REMOVE-PEER <table> <shard-id> <peer-id>
func (m *Manager) handleShardRemovePeer(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 5 {
		return fail(req, errors.New("usage: ADMIN SHARD-REMOVE-PEER <table> <shard-id> <peer-id>"))
	}
	_, shardID, peerID := fields[2], fields[3], fields[4]
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	if !sh.node.IsLeader() {
		addr, err := m.shardLeaderSQLAddr(sh, 5*time.Second)
		if err != nil {
			return fail(req, err)
		}
		if addr != m.sqlAddr {
			resp, err := m.remoteAdmin(addr, req.SQL)
			if err != nil {
				return fail(req, err)
			}
			resp.ID = req.ID
			return resp
		}
	}
	if err := sh.node.RemovePeer(peerID); err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleShardPeers lists the local member peers of a shard group.
// Syntax: ADMIN SHARD-PEERS <table> <shard-id>
func (m *Manager) handleShardPeers(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return fail(req, errors.New("usage: ADMIN SHARD-PEERS <table> <shard-id>"))
	}
	shardID := fields[3]
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	peers := sh.node.Peers()
	rows := make([][]string, 0, len(peers))
	for _, p := range peers {
		rows = append(rows, []string{p.ID, string(p.Address)})
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "peers", Rows: rows, Columns: []string{"ID", "Addr"}}}
}

// handleShardWaitLeader blocks until the local replica of a shard group knows
// its leader (used while assembling a fresh shard group).
func (m *Manager) handleShardWaitLeader(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return fail(req, errors.New("usage: ADMIN SHARD-WAIT-LEADER <table> <shard-id>"))
	}
	shardID := fields[3]
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	if err := waitGroupLeader(sh.node.Group(), 10*time.Second); err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleExecShard applies a DML statement directly to a local shard group.
// Syntax: ADMIN EXEC-SHARD <table> <shard-id> <sql...>
func (m *Manager) handleExecShard(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) < 4 {
		return fail(req, errors.New("usage: ADMIN EXEC-SHARD <table> <shard-id> <sql>"))
	}
	table, shardID := fields[2], fields[3]
	body := strings.TrimSpace(strings.TrimPrefix(req.SQL, "ADMIN EXEC-SHARD "+table+" "+shardID))
	_ = table
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	if sh.frozen {
		return fail(req, fmt.Errorf("shard %s is splitting, retry", shardID))
	}
	if !sh.node.IsLeader() {
		addr, err := m.shardLeaderSQLAddr(sh, 5*time.Second)
		if err != nil {
			return fail(req, err)
		}
		if addr != m.sqlAddr {
			resp, err := m.remoteAdmin(addr, req.SQL)
			if err != nil {
				return fail(req, err)
			}
			resp.ID = req.ID
			return resp
		}
		// we are the leader but the election just finished; fall through
	}
	r, err := sh.node.Execute(body)
	if err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: payloadOf(r)}
}

// handleScanShard returns all rows of one shard (for SELECT scatter).
func (m *Manager) handleScanShard(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return fail(req, errors.New("usage: ADMIN SCAN-SHARD <table> <shard-id>"))
	}
	table, shardID := fields[2], fields[3]
	_ = table
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	r, err := sh.node.Execute("SELECT * FROM " + table)
	if err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: payloadOf(r)}
}

// handleFreezeShard rejects (or re-enables) writes to a shard during a split.
func (m *Manager) handleFreezeShard(req *proto.Request, freeze bool) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return fail(req, errors.New("usage: ADMIN FREEZE-SHARD <table> <shard-id>"))
	}
	shardID := fields[3]
	sh := m.localShard(shardID)
	if sh == nil {
		return fail(req, fmt.Errorf("shard %s not mounted locally", shardID))
	}
	sh.frozen = !freeze
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleUnmountShard closes and removes the local replica of a shard.
func (m *Manager) handleUnmountShard(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 4 {
		return fail(req, errors.New("usage: ADMIN UNMOUNT-SHARD <table> <shard-id>"))
	}
	shardID := fields[3]
	m.mu.Lock()
	sh, ok := m.shards[shardID]
	if ok {
		delete(m.shards, shardID)
	}
	m.mu.Unlock()
	if ok {
		_ = sh.node.Close()
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// handleShards lists a table's shards.
func (m *Manager) handleShards(req *proto.Request) *proto.Response {
	fields := strings.Fields(req.SQL)
	if len(fields) != 3 {
		return fail(req, errors.New("usage: ADMIN SHARDS <table>"))
	}
	ts := m.table(fields[2])
	if ts == nil {
		return fail(req, notFound(fields[2]))
	}
	p := &proto.ResultPayload{Type: "select", Columns: []string{"shard", "start", "end", "nodes", "size"}}
	for _, s := range ts.Shards {
		p.Rows = append(p.Rows, []string{
			s.ID,
			string(s.Start),
			string(s.End),
			strings.Join(s.Nodes, ","),
			strconv.FormatUint(s.Size, 10),
		})
	}
	return &proto.Response{ID: req.ID, OK: true, Result: p}
}

// remoteAdmin sends an ADMIN/SQL request to another node's SQL server.
func (m *Manager) remoteAdmin(addr, sql string) (*proto.Response, error) {
	var resp *proto.Response
	err := m.pool.Do(addr, func(c *proto.Client) error {
		r, err := c.Execute(sql)
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// --- DDL ---

func (m *Manager) ddlCreateTable(st *sqlx.CreateTableStmt) (*sqlx.Result, error) {
	schema := normalizeSchema(st)
	if !m.meta.IsLeader() {
		resp := m.forwardToLeaderSQL(nil, sqlx.StatementString(st))
		if resp == nil {
			return nil, errors.New("forward failed")
		}
		if !resp.OK {
			return nil, errors.New(resp.Error)
		}
		return &sqlx.Result{Type: "create_table"}, nil
	}
	if err := m.meta.CreateTableOn(schema, m.rf, m.liveNodes()); err != nil {
		return nil, err
	}
	ts := m.table(schema.Name)
	if ts == nil {
		return nil, fmt.Errorf("table %q missing after commit", schema.Name)
	}
	if err := m.orchestrateMounts(ts); err != nil {
		return nil, err
	}
	return &sqlx.Result{Type: "create_table", Affected: 1}, nil
}

func (m *Manager) ddlDropTable(st *sqlx.DropTableStmt) (*sqlx.Result, error) {
	if !m.meta.IsLeader() {
		resp := m.forwardToLeaderSQL(nil, sqlx.StatementString(st))
		if resp == nil {
			return nil, errors.New("forward failed")
		}
		if !resp.OK {
			return nil, errors.New(resp.Error)
		}
		return &sqlx.Result{Type: "drop_table"}, nil
	}
	if err := m.meta.DropTable(st.Name); err != nil {
		return nil, err
	}
	return &sqlx.Result{Type: "drop_table", Affected: 1}, nil
}

// normalizeSchema mirrors the executor's CREATE TABLE normalization.
func normalizeSchema(st *sqlx.CreateTableStmt) *sqlx.TableSchema {
	cols := st.Columns
	if st.PK != nil && len(st.PK) > 0 {
		for i := range cols {
			for _, p := range st.PK {
				if cols[i].Name == p {
					cols[i].AsPrimary = true
					cols[i].NotNull = true
				}
			}
		}
	}
	hasPK := false
	for _, c := range cols {
		if c.AsPrimary {
			hasPK = true
			break
		}
	}
	if !hasPK && len(cols) > 0 {
		cols[0].AsPrimary = true
		cols[0].NotNull = true
	}
	pk := []string{}
	for _, c := range cols {
		if c.AsPrimary {
			pk = append(pk, c.Name)
		}
	}
	return &sqlx.TableSchema{Name: st.Name, Columns: cols, PK: pk}
}

// orchestrateMounts tells each placement node to mount its copy of the shard
// and joins the shard group together.
func (m *Manager) orchestrateMounts(ts *TableSpec) error {
	for _, spec := range ts.Shards {
		if err := m.mountShardGroup(spec); err != nil {
			return err
		}
	}
	return nil
}

// mountShardGroup mounts a shard on all its placement nodes and joins the
// group: node[0] bootstraps and becomes leader, the rest join as followers.
func (m *Manager) mountShardGroup(spec *ShardSpec) error {
	addrs := make([]string, len(spec.Nodes))
	for i, nid := range spec.Nodes {
		addr := m.nodeSQLAddr(nid)
		if addr == "" {
			return fmt.Errorf("placement node %s not registered", nid)
		}
		if addr == m.sqlAddr {
			sh, err := m.mountShard(spec, i == 0)
			if err != nil {
				return err
			}
			addrs[i] = sh.node.Group().Advertise()
			continue
		}
		resp, err := m.remoteAdmin(addr, fmt.Sprintf("ADMIN MOUNT-SHARD %s %s %s %s %v %s",
			spec.Table, spec.ID, encodeKey(spec.Start), encodeKey(spec.End), i == 0, strings.Join(spec.Nodes, " ")))
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New(resp.Error)
		}
		addrs[i] = resp.Result.Note
	}
	// node[0] bootstrapped the group; make sure it has a leader before joining
	// followers, otherwise AddVoter would be called on a non-leader.
	firstAddr := m.nodeSQLAddr(spec.Nodes[0])
	if firstAddr == "" {
		return fmt.Errorf("placement node %s not registered", spec.Nodes[0])
	}
	if firstAddr == m.sqlAddr {
		sh := m.localShard(spec.ID)
		if sh == nil {
			return fmt.Errorf("shard %s not mounted locally", spec.ID)
		}
		if err := waitGroupLeader(sh.node.Group(), 10*time.Second); err != nil {
			return fmt.Errorf("shard %s: %w", spec.ID, err)
		}
	} else {
		resp, err := m.remoteAdmin(firstAddr, fmt.Sprintf("ADMIN SHARD-WAIT-LEADER %s %s", spec.Table, spec.ID))
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New(resp.Error)
		}
	}
	// join every follower into node[0]'s group
	for i := 1; i < len(spec.Nodes); i++ {
		resp, err := m.remoteAdmin(firstAddr, fmt.Sprintf("ADMIN SHARD-ADD-PEER %s %s %s %s",
			spec.Table, spec.ID, spec.Nodes[i], addrs[i]))
		if err != nil {
			return err
		}
		if !resp.OK {
			return errors.New(resp.Error)
		}
	}
	return nil
}

func encodeKey(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	return base64.StdEncoding.EncodeToString(b)
}

func decodeKey(s string) ([]byte, error) {
	if s == "-" || s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}

// --- DML routing ---

func (m *Manager) execInsert(st *sqlx.InsertStmt) (*sqlx.Result, error) {
	ts := m.table(st.Table)
	if ts == nil {
		return nil, notFound(st.Table)
	}
	schema := ts.Schema
	colIdx := map[string]int{}
	if len(st.Columns) == 0 {
		for i, c := range schema.Columns {
			colIdx[c.Name] = i
		}
	} else {
		for i, c := range st.Columns {
			colIdx[c] = i
		}
	}
	// group rows by owning shard so each shard receives ONE batched INSERT,
	// turning N raft.Apply + N network hops into a single shard-local Apply
	byShard := map[*ShardSpec][][]sqlx.Expr{}
	var order []*ShardSpec
	for _, row := range st.Rows {
		pkKey, err := pkKeyForRow(schema, colIdx, row)
		if err != nil {
			return nil, err
		}
		spec := m.owningShard(st.Table, pkKey)
		if spec == nil {
			return nil, fmt.Errorf("no shard owns key for table %q", st.Table)
		}
		if _, ok := byShard[spec]; !ok {
			order = append(order, spec)
		}
		byShard[spec] = append(byShard[spec], row)
	}
	affected := int64(0)
	for _, spec := range order {
		batch := &sqlx.InsertStmt{Table: st.Table, Columns: st.Columns, Rows: byShard[spec]}
		resp, err := m.execShardSQL(spec, sqlx.StatementString(batch))
		if err != nil {
			return nil, err
		}
		if !resp.OK {
			return nil, errors.New(resp.Error)
		}
		affected += int64(len(batch.Rows))
	}
	return &sqlx.Result{Type: "insert", Affected: affected}, nil
}

func (m *Manager) execUpdate(st *sqlx.UpdateStmt) (*sqlx.Result, error) {
	return m.execWriteDML(st.Table, sqlx.StatementString(st), st.Where, "update")
}

func (m *Manager) execDelete(st *sqlx.DeleteStmt) (*sqlx.Result, error) {
	return m.execWriteDML(st.Table, sqlx.StatementString(st), st.Where, "delete")
}

// execWriteDML routes UPDATE/DELETE to one shard (if WHERE pins the PK) or
// broadcasts to every shard of the table.
func (m *Manager) execWriteDML(table, sql string, where sqlx.Expr, typ string) (*sqlx.Result, error) {
	ts := m.table(table)
	if ts == nil {
		return nil, notFound(table)
	}
	var affected int64
	key, pinned := pkKeyFromWhere(ts.Schema, where)
	targets := []*ShardSpec{}
	if pinned {
		spec := m.owningShard(table, key)
		if spec == nil {
			return nil, notFound(table)
		}
		targets = append(targets, spec)
	} else {
		targets = ts.Shards
	}
	for _, spec := range targets {
		resp, err := m.execShardSQL(spec, sql)
		if err != nil {
			return nil, err
		}
		if !resp.OK {
			return nil, errors.New(resp.Error)
		}
		if resp.Result != nil {
			affected += resp.Result.Affected
		}
	}
	return &sqlx.Result{Type: typ, Affected: affected}, nil
}

// execShardSQL executes a shard-scoped DML via the shard's leader, avoiding
// re-routing loops.
func (m *Manager) execShardSQL(spec *ShardSpec, sql string) (*proto.Response, error) {
	if !m.hosts(spec) {
		addr := m.livePlacementAddr(spec)
		if addr == "" {
			return nil, fmt.Errorf("shard %s has no live placement", spec.ID)
		}
		return m.remoteAdmin(addr, fmt.Sprintf("ADMIN EXEC-SHARD %s %s %s", spec.Table, spec.ID, sql))
	}
	sh := m.localShard(spec.ID)
	if sh == nil {
		return nil, fmt.Errorf("shard %s not mounted locally", spec.ID)
	}
	if !sh.node.IsLeader() {
		addr, err := m.shardLeaderSQLAddr(sh, 5*time.Second)
		if err != nil {
			return nil, err
		}
		if addr != m.sqlAddr {
			return m.remoteAdmin(addr, fmt.Sprintf("ADMIN EXEC-SHARD %s %s %s", spec.Table, spec.ID, sql))
		}
		// we are the leader but the election just finished; fall through
	}
	r, err := sh.node.Execute(sql)
	if err != nil {
		return nil, err
	}
	return &proto.Response{OK: true, Result: payloadOf(r)}, nil
}

func (m *Manager) execSelect(st *sqlx.SelectStmt) (*sqlx.Result, error) {
	if st.From == "" {
		return sqlx.NewExecutor(emptyEngine{}).Execute(st)
	}
	ts := m.table(st.From)
	if ts == nil {
		return nil, notFound(st.From)
	}
	u := &unionEngine{manager: m, spec: ts}
	seen := map[string]bool{}
	// scan narrowest (hottest) shards first so their values win on overlap
	shards := make([]*ShardSpec, len(ts.Shards))
	copy(shards, ts.Shards)
	sort.SliceStable(shards, func(i, j int) bool { return bytes.Compare(shards[i].Start, shards[j].Start) > 0 })
	for _, spec := range shards {
		rows, err := m.scanShard(spec)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			k, err := pkKeyForRowValues(ts.Schema, r)
			if err != nil {
				return nil, err
			}
			if seen[k] {
				continue
			}
			seen[k] = true
			u.rows = append(u.rows, r)
		}
	}
	return sqlx.NewExecutor(u).Execute(st)
}

// --- key helpers ---

func colTypeOf(schema *sqlx.TableSchema, name string) sqlx.Type {
	for _, c := range schema.Columns {
		if c.Name == name {
			return c.Type
		}
	}
	return sqlx.TypeNull
}

func zeroForType(t sqlx.Type) sqlx.Value {
	switch t {
	case sqlx.TypeInt:
		return sqlx.IntValue(0)
	case sqlx.TypeFloat:
		return sqlx.FloatValue(0)
	case sqlx.TypeString:
		return sqlx.StrValue("")
	case sqlx.TypeBool:
		return sqlx.BoolValue(false)
	case sqlx.TypeTimestamp:
		return sqlx.TimestampValue(time.Unix(0, 0))
	}
	return sqlx.NullValue
}

// pkKeyForRow computes the encoded PK key for one INSERT row.
func pkKeyForRow(schema *sqlx.TableSchema, colIdx map[string]int, row []sqlx.Expr) (string, error) {
	pk := make([]sqlx.Value, 0, len(schema.PK))
	for _, p := range schema.PK {
		idx, ok := colIdx[p]
		var v sqlx.Value
		if !ok {
			v = zeroForType(colTypeOf(schema, p))
		} else {
			e, err := sqlx.Eval(row[idx], nil)
			if err != nil {
				return "", err
			}
			v, err = sqlx.Convert(e, colTypeOf(schema, p))
			if err != nil {
				return "", err
			}
		}
		pk = append(pk, v)
	}
	return storage.EncodePK(pk), nil
}

// pkKeyFromWhere extracts an equality constraint on the full PK from a WHERE
// expression, returning the encoded key if the row is fully pinned.
func pkKeyFromWhere(schema *sqlx.TableSchema, where sqlx.Expr) (string, bool) {
	if where == nil || len(schema.PK) == 0 {
		return "", false
	}
	found := map[string]sqlx.Value{}
	var walk func(e sqlx.Expr) bool
	walk = func(e sqlx.Expr) bool {
		switch b := e.(type) {
		case *sqlx.BinaryOp:
			if b.Op == "AND" {
				return walk(b.Left) && walk(b.Right)
			}
			if b.Op == "=" {
				if id, ok := b.Left.(*sqlx.Ident); ok {
					v, err := sqlx.Eval(b.Right, nil)
					if err == nil {
						found[id.Name] = v
						return true
					}
				}
				if id, ok := b.Right.(*sqlx.Ident); ok {
					v, err := sqlx.Eval(b.Left, nil)
					if err == nil {
						found[id.Name] = v
						return true
					}
				}
			}
		}
		return false
	}
	if !walk(where) {
		return "", false
	}
	pk := make([]sqlx.Value, 0, len(schema.PK))
	for _, p := range schema.PK {
		v, ok := found[p]
		if !ok {
			return "", false
		}
		v, err := sqlx.Convert(v, colTypeOf(schema, p))
		if err != nil {
			return "", false
		}
		pk = append(pk, v)
	}
	return storage.EncodePK(pk), true
}

// --- union engine (scatter/gather SELECT) ---

type unionEngine struct {
	manager *Manager
	spec    *TableSpec
	rows    []sqlx.Row
}

func (u *unionEngine) GetSchema(name string) (*sqlx.TableSchema, error) {
	if name != u.spec.Schema.Name {
		return nil, notFound(name)
	}
	return u.spec.Schema, nil
}
func (u *unionEngine) Scan(name string) ([]sqlx.Row, error) {
	if name != u.spec.Schema.Name {
		return nil, notFound(name)
	}
	return u.rows, nil
}
func (u *unionEngine) CreateTable(*sqlx.TableSchema) error { return errors.New("read-only") }
func (u *unionEngine) DropTable(string) error              { return errors.New("read-only") }
func (u *unionEngine) Insert(string, map[string]sqlx.Value) error {
	return errors.New("read-only")
}
func (u *unionEngine) Update(string, []sqlx.Value, map[string]sqlx.Value) error {
	return errors.New("read-only")
}
func (u *unionEngine) Delete(string, []sqlx.Value) error { return errors.New("read-only") }

type emptyEngine struct{}

func (emptyEngine) GetSchema(name string) (*sqlx.TableSchema, error) { return nil, notFound(name) }
func (emptyEngine) Scan(name string) ([]sqlx.Row, error)             { return nil, notFound(name) }
func (emptyEngine) CreateTable(*sqlx.TableSchema) error              { return errors.New("read-only") }
func (emptyEngine) DropTable(string) error                           { return errors.New("read-only") }
func (emptyEngine) Insert(string, map[string]sqlx.Value) error       { return errors.New("read-only") }
func (emptyEngine) Update(string, []sqlx.Value, map[string]sqlx.Value) error {
	return errors.New("read-only")
}
func (emptyEngine) Delete(string, []sqlx.Value) error { return errors.New("read-only") }

// rowsFromPayload converts SCAN-SHARD string rows back into typed values.
func rowsFromPayload(p *proto.ResultPayload, schema *sqlx.TableSchema) []sqlx.Row {
	colIdx := map[string]int{}
	for i, name := range p.Columns {
		colIdx[name] = i
	}
	var out []sqlx.Row
	for _, sr := range p.Rows {
		row := make(sqlx.Row, len(schema.Columns))
		for i, c := range schema.Columns {
			ci, ok := colIdx[c.Name]
			if !ok || ci >= len(sr) {
				row[i] = zeroForType(c.Type)
				continue
			}
			row[i] = valueFromString(c.Type, sr[ci])
		}
		out = append(out, row)
	}
	return out
}

func valueFromString(t sqlx.Type, s string) sqlx.Value {
	switch t {
	case sqlx.TypeInt:
		if s == "" {
			return sqlx.NullValue
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sqlx.NullValue
		}
		return sqlx.IntValue(v)
	case sqlx.TypeFloat:
		if s == "" {
			return sqlx.NullValue
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return sqlx.NullValue
		}
		return sqlx.FloatValue(v)
	case sqlx.TypeString:
		return sqlx.StrValue(s)
	case sqlx.TypeBool:
		return sqlx.BoolValue(s == "true" || s == "1")
	case sqlx.TypeTimestamp:
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return sqlx.NullValue
		}
		return sqlx.TimestampValue(t)
	}
	return sqlx.NullValue
}

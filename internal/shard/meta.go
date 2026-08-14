// Package shard implements the YDB-style sharding layer: a replicated
// catalog (meta group) plus per-shard Raft data groups.
package shard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"ydbgo/internal/raftsvc"
	sqlx "ydbgo/internal/sql"
)

// NodeSpec describes a physical node participating in the cluster.
type NodeSpec struct {
	ID       string `json:"id"`
	SQLAddr  string `json:"sql_addr"`  // TCP address of the node's SQL server
	RaftAddr string `json:"raft_addr"` // transport address (meta group + data shards)
}

// ShardSpec is a data shard: a Raft group over a primary-key range.
type ShardSpec struct {
	ID    string   `json:"id"` // e.g. "users-0", "users-0-1"
	Table string   `json:"table"`
	Start []byte   `json:"start,omitempty"` // inclusive encoded PK; nil = -inf
	End   []byte   `json:"end,omitempty"`   // exclusive encoded PK; nil = +inf
	Nodes []string `json:"nodes"`           // node IDs hosting the shard group (RF)
	Size  uint64   `json:"size"`            // last known estimated size
}

// Owns reports whether the encoded PK key belongs to this shard's range.
func (s *ShardSpec) Owns(key string) bool {
	if len(s.Start) > 0 && key < string(s.Start) {
		return false
	}
	if len(s.End) > 0 && key >= string(s.End) {
		return false
	}
	return true
}

// StartAfter reports whether this shard's range is more specific (narrower,
// i.e. larger Start) than o, for overlapping ranges produced by splits.
func (s *ShardSpec) StartAfter(o *ShardSpec) bool {
	// a wider Start means a narrower (hotter) slice
	return bytes.Compare(s.Start, o.Start) > 0
}

// TableSpec is a table and its shards.
type TableSpec struct {
	Schema *sqlx.TableSchema `json:"schema"`
	Shards []*ShardSpec      `json:"shards"`
}

// Catalog is the replicated shard catalog (analogous to YDB SchemeShard/Hive).
type Catalog struct {
	Version uint64                `json:"version"`
	Nodes   []string              `json:"nodes"` // ordered node IDs
	Specs   map[string]*NodeSpec  `json:"specs"`
	Tables  map[string]*TableSpec `json:"tables"`
}

// MetaCommand is a replicated mutation of the catalog.
type MetaCommand struct {
	Op string `json:"op"` // register_node | create_table | drop_table | split_shard | remove_node | set_shard_nodes

	// register_node
	ID       string `json:"id,omitempty"`
	SQLAddr  string `json:"sql_addr,omitempty"`
	RaftAddr string `json:"raft_addr,omitempty"`

	// create_table
	Schema *sqlx.TableSchema `json:"schema,omitempty"`
	Shards []*ShardSpec      `json:"shards,omitempty"`

	// drop_table / split_shard
	Table     string       `json:"table,omitempty"`
	Shard     string       `json:"shard,omitempty"`
	NewShards []*ShardSpec `json:"new_shards,omitempty"`
	SplitKey  string       `json:"split_key,omitempty"`

	// set_shard_nodes
	Nodes []string `json:"nodes,omitempty"`

	// remove_node
	RemoveID string `json:"remove_id,omitempty"`
}

// MetaFSM is the raft.FSM that owns the catalog.
type MetaFSM struct {
	mu  sync.RWMutex
	cat *Catalog
	dir string
}

func NewMetaFSM(dir string) *MetaFSM {
	f := &MetaFSM{
		cat: &Catalog{
			Specs:  map[string]*NodeSpec{},
			Tables: map[string]*TableSpec{},
		},
		dir: dir,
	}
	f.load()
	return f
}

// Catalog returns the current catalog. The returned object is immutable:
// every Apply builds a fresh catalog copy, so readers never observe a
// mutating catalog and no serialization happens on the read path.
func (f *MetaFSM) Catalog() *Catalog {
	f.mu.RLock()
	c := f.cat
	f.mu.RUnlock()
	return c
}

func (f *MetaFSM) Apply(l *raft.Log) interface{} {
	var cmd MetaCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("meta: bad command: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	next := cloneCatalog(f.cat)
	if err := applyCommand(next, &cmd); err != nil {
		return err
	}
	next.Version++
	f.cat = next
	if err := f.persist(); err != nil {
		return err
	}
	return nil
}

func applyCommand(cat *Catalog, cmd *MetaCommand) error {
	switch cmd.Op {
	case "register_node":
		if _, ok := cat.Specs[cmd.ID]; !ok {
			cat.Specs[cmd.ID] = &NodeSpec{ID: cmd.ID, SQLAddr: cmd.SQLAddr, RaftAddr: cmd.RaftAddr}
			cat.Nodes = append(cat.Nodes, cmd.ID)
		}
	case "remove_node":
		if _, ok := cat.Specs[cmd.RemoveID]; ok {
			delete(cat.Specs, cmd.RemoveID)
			cat.Nodes = removeString(cat.Nodes, cmd.RemoveID)
		}
	case "set_shard_nodes":
		ts, ok := cat.Tables[cmd.Table]
		if !ok {
			return fmt.Errorf("table %q not found", cmd.Table)
		}
		for _, s := range ts.Shards {
			if s.ID == cmd.Shard {
				s.Nodes = append([]string(nil), cmd.Nodes...)
				return nil
			}
		}
		return fmt.Errorf("shard %q not found", cmd.Shard)
	case "create_table":
		// idempotent so a log replay on restart does not duplicate the table
		if _, ok := cat.Tables[cmd.Schema.Name]; ok {
			return nil
		}
		ts := &TableSpec{Schema: cmd.Schema, Shards: cmd.Shards}
		for _, s := range ts.Shards {
			s.Table = cmd.Schema.Name
		}
		cat.Tables[cmd.Schema.Name] = ts
	case "drop_table":
		delete(cat.Tables, cmd.Table)
	case "split_shard":
		// The old shard keeps its range and its physical data; a new EMPTY
		// shard is appended for the hot range. No migration happens.
		ts, ok := cat.Tables[cmd.Table]
		if !ok {
			return fmt.Errorf("table %q not found", cmd.Table)
		}
		for _, s := range cmd.NewShards {
			// idempotent: skip shards already added by a prior replay
			if shardExists(ts.Shards, s.ID) {
				continue
			}
			s.Table = cmd.Table
			ts.Shards = append(ts.Shards, s)
		}
	default:
		return fmt.Errorf("meta: unknown op %q", cmd.Op)
	}
	return nil
}

// persist writes the catalog to disk atomically.
func (f *MetaFSM) persist() error {
	if f.dir == "" {
		return nil
	}
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(f.cat)
	if err != nil {
		return err
	}
	tmp := filepath.Join(f.dir, "catalog.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(f.dir, "catalog.json"))
}

func (f *MetaFSM) load() {
	if f.dir == "" {
		return
	}
	b, err := os.ReadFile(filepath.Join(f.dir, "catalog.json"))
	if err != nil {
		return
	}
	var cat Catalog
	if err := json.Unmarshal(b, &cat); err == nil {
		if cat.Specs == nil {
			cat.Specs = map[string]*NodeSpec{}
		}
		if cat.Tables == nil {
			cat.Tables = map[string]*TableSpec{}
		}
		f.cat = &cat
	}
}

// Snapshot implements raft.FSM.
func (f *MetaFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := json.Marshal(f.cat)
	if err != nil {
		return nil, err
	}
	return &metaSnapshot{data: b}, nil
}

// Restore implements raft.FSM.
func (f *MetaFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	var cat Catalog
	if err := json.Unmarshal(b, &cat); err != nil {
		return err
	}
	if cat.Specs == nil {
		cat.Specs = map[string]*NodeSpec{}
	}
	if cat.Tables == nil {
		cat.Tables = map[string]*TableSpec{}
	}
	f.mu.Lock()
	f.cat = &cat
	f.mu.Unlock()
	return f.persist()
}

type metaSnapshot struct{ data []byte }

func (s *metaSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *metaSnapshot) Release() {}

func cloneCatalog(cat *Catalog) *Catalog {
	b, _ := json.Marshal(cat)
	var out Catalog
	_ = json.Unmarshal(b, &out)
	return &out
}

func shardExists(shards []*ShardSpec, id string) bool {
	for _, s := range shards {
		if s.ID == id {
			return true
		}
	}
	return false
}

func removeString(xs []string, v string) []string {
	for i, x := range xs {
		if x == v {
			return append(xs[:i], xs[i+1:]...)
		}
	}
	return xs
}

// MetaNode wraps a meta Raft group and its catalog FSM.
type MetaNode struct {
	fsm *MetaFSM
	grp *raftsvc.Group
}

// NewMetaNode creates a meta node (group not started yet).
func NewMetaNode(id, raftAddr, dir string) *MetaNode {
	fsm := NewMetaFSM(dir)
	return &MetaNode{fsm: fsm, grp: raftsvc.NewGroup(id, raftAddr, dir, fsm)}
}

func (m *MetaNode) Start(bootstrap bool, peers []raft.Server) error {
	return m.grp.Start(bootstrap, peers)
}

func (m *MetaNode) FSM() *MetaFSM         { return m.fsm }
func (m *MetaNode) Group() *raftsvc.Group { return m.grp }
func (m *MetaNode) IsLeader() bool        { return m.grp.IsLeader() }
func (m *MetaNode) LeaderAddr() string    { return m.grp.LeaderAddr() }
func (m *MetaNode) LeaderID() string      { return m.grp.LeaderID() }
func (m *MetaNode) Peers() []raftsvc.Peer { return m.grp.Peers() }

// RegisterNode proposes this node into the catalog.
func (m *MetaNode) RegisterNode(id, sqlAddr, raftAddr string) error {
	return m.propose(&MetaCommand{Op: "register_node", ID: id, SQLAddr: sqlAddr, RaftAddr: raftAddr})
}

// CreateTable proposes a new table with an initial shard placed on rf nodes.
// The leader computes placement from the current catalog, so all replicas
// apply identical payloads.
func (m *MetaNode) CreateTable(schema *sqlx.TableSchema, rf int) error {
	return m.createTableOn(schema, rf, nil)
}

// CreateTableOn is CreateTable but places the initial shard only on the given
// candidate nodes (e.g. the currently-live ones). nil/empty falls back to the
// whole catalog.
func (m *MetaNode) CreateTableOn(schema *sqlx.TableSchema, rf int, candidates []string) error {
	return m.createTableOn(schema, rf, candidates)
}

func (m *MetaNode) createTableOn(schema *sqlx.TableSchema, rf int, candidates []string) error {
	if !m.IsLeader() {
		return errors.New("meta: not leader")
	}
	cat := m.fsm.Catalog()
	if len(candidates) == 0 {
		candidates = cat.Nodes
	}
	if rf <= 0 || rf > len(candidates) {
		rf = len(candidates)
	}
	nodes := pickNodes(candidates, rf, hashOf(schema.Name+"-0"))
	if len(nodes) == 0 {
		return errors.New("meta: no nodes registered for shard placement")
	}
	shard := &ShardSpec{ID: schema.Name + "-0", Start: nil, End: nil, Nodes: nodes}
	return m.propose(&MetaCommand{Op: "create_table", Schema: schema, Shards: []*ShardSpec{shard}})
}

// DropTable proposes dropping a table and all its shards.
func (m *MetaNode) DropTable(name string) error {
	return m.propose(&MetaCommand{Op: "drop_table", Table: name})
}

// SplitShard appends new shard specs to a table (a split adds an empty shard
// for the hot range without migrating rows).
func (m *MetaNode) SplitShard(table, splitKey string, newShards []*ShardSpec) error {
	return m.propose(&MetaCommand{Op: "split_shard", Table: table, SplitKey: splitKey, NewShards: newShards})
}

// SetShardNodes proposes replacing a shard's placement nodes (used by the
// recovery coordinator after a replica is healed onto another live node).
func (m *MetaNode) SetShardNodes(table, shardID string, nodes []string) error {
	return m.propose(&MetaCommand{Op: "set_shard_nodes", Table: table, Shard: shardID, Nodes: nodes})
}

// propose applies a command through Raft and waits for commit.
func (m *MetaNode) propose(cmd *MetaCommand) error {
	if !m.IsLeader() {
		return errors.New("meta: not leader")
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := m.grp.Raft().Apply(b, 10*time.Second)
	if f.Error() != nil {
		return f.Error()
	}
	if f.Response() != nil {
		if e, ok := f.Response().(error); ok {
			return e
		}
	}
	return nil
}

// Close shuts down the meta group.
func (m *MetaNode) Close() error { return m.grp.Close() }

// placement

// pickNodes selects up to n node IDs by consistent hash of seed.
func pickNodes(all []string, n int, seed uint64) []string {
	if n <= 0 || len(all) == 0 {
		return nil
	}
	if n > len(all) {
		n = len(all)
	}
	type nd struct {
		hash uint64
		id   string
	}
	arr := make([]nd, 0, len(all))
	for _, id := range all {
		h := fnv.New64a()
		h.Write([]byte(fmt.Sprintf("%s:%d", id, seed)))
		arr = append(arr, nd{hash: h.Sum64(), id: id})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].hash < arr[j].hash })
	out := make([]string, 0, n)
	for _, d := range arr {
		out = append(out, d.id)
		if len(out) == n {
			break
		}
	}
	return out
}

func hashOf(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

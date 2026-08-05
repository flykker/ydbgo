package shard

import (
	"bytes"
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

// handleSplit triggers a manual split: ADMIN SPLIT TABLE <table> AT <pk-value>
func (m *Manager) handleSplit(req *proto.Request) *proto.Response {
	if !m.meta.IsLeader() {
		return m.forwardToLeaderSQL(req, req.SQL)
	}
	fields := strings.Fields(req.SQL)
	if len(fields) != 6 || strings.ToUpper(fields[2]) != "TABLE" || strings.ToUpper(fields[4]) != "AT" {
		return fail(req, errors.New("usage: ADMIN SPLIT TABLE <table> AT <pk-value>"))
	}
	table := fields[3]
	pkVal := fields[5]
	if err := m.splitShardByValue(table, pkVal); err != nil {
		return fail(req, err)
	}
	return &proto.Response{ID: req.ID, OK: true, Result: &proto.ResultPayload{Type: "admin"}}
}

// monitorLoop periodically checks shard sizes and splits oversized shards.
func (m *Manager) monitorLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.splitTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.maybeAutoSplit()
		}
	}
}

func (m *Manager) maybeAutoSplit() {
	if m.shardSize == 0 || !m.meta.IsLeader() {
		return
	}
	cat := m.meta.FSM().Catalog()
	for _, ts := range cat.Tables {
		for _, spec := range ts.Shards {
			if !m.hosts(spec) {
				continue
			}
			// skip a shard that already has a narrower hot-range successor
			// sharing the same End (it no longer receives new keys)
			if hasNarrowerSucc(ts.Shards, spec) {
				continue
			}
			sh := m.localShard(spec.ID)
			if sh == nil {
				continue
			}
			size := sh.node.Engine().EstimateSize()
			if size < m.shardSize {
				continue
			}
			key, ok := m.splitPointFor(spec)
			if !ok {
				continue
			}
			_ = m.splitShard(spec, key)
		}
	}
}

// hasNarrowerSucc reports whether shard already has a narrower slice (larger
// Start, same End) which now receives its hot keys.
func hasNarrowerSucc(shards []*ShardSpec, spec *ShardSpec) bool {
	for _, s := range shards {
		if s.ID == spec.ID || !bytes.Equal(s.End, spec.End) {
			continue
		}
		if bytes.Compare(s.Start, spec.Start) > 0 {
			return true
		}
	}
	return false
}

// splitShardByValue adds a new empty shard split off spec at the given PK.
func (m *Manager) splitShardByValue(table, pkVal string) error {
	ts := m.table(table)
	if ts == nil {
		return notFound(table)
	}
	if len(ts.Schema.PK) != 1 {
		return errors.New("split by value requires a single-column primary key")
	}
	col := ts.Schema.PK[0]
	v, err := parseValueString(colTypeOf(ts.Schema, col), pkVal)
	if err != nil {
		return err
	}
	key := storage.EncodePK([]sqlx.Value{v})
	spec := m.owningShard(table, key)
	if spec == nil {
		return notFound(table)
	}
	if len(spec.Start) > 0 && bytes.Compare([]byte(key), spec.Start) <= 0 {
		return fmt.Errorf("split key %q is below shard %s start", pkVal, spec.ID)
	}
	return m.splitShard(spec, key)
}

// splitPointFor picks a median PK key of a shard's rows as a split point.
func (m *Manager) splitPointFor(spec *ShardSpec) (string, bool) {
	ts := m.table(spec.Table)
	if ts == nil {
		return "", false
	}
	rows, err := m.scanShard(spec)
	if err != nil {
		return "", false
	}
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		k, err := pkKeyForRowValues(ts.Schema, r)
		if err != nil {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) < 2 {
		return "", false
	}
	sort.Strings(keys)
	mid := keys[len(keys)/2]
	// median must actually split the shard (a successor has to be able to own
	// keys >= mid, newly written)
	if bytes.Compare([]byte(mid), []byte(spec.Start)) <= 0 {
		return "", false
	}
	return mid, true
}

// splitShard appends a new EMPTY shard for [splitKey, spec.End). The old shard
// keeps its range and data; no rows are migrated.
func (m *Manager) splitShard(spec *ShardSpec, splitKey string) error {
	ts := m.table(spec.Table)
	if ts == nil {
		return notFound(spec.Table)
	}
	cat := m.meta.FSM().Catalog()
	rf := m.rf
	if rf <= 0 {
		rf = len(cat.Nodes)
	}
	// place new shards only on currently-live nodes
	candidates := m.liveNodes()
	if len(candidates) == 0 {
		candidates = cat.Nodes
	}
	if rf > len(candidates) {
		rf = len(candidates)
	}
	newSpec := &ShardSpec{
		ID:    spec.ID + "-" + strconv.Itoa(len(ts.Shards)),
		Table: spec.Table,
		Start: []byte(splitKey),
		End:   append([]byte(nil), spec.End...),
		Nodes: pickNodes(candidates, rf, hashOf(spec.ID+"-"+strconv.Itoa(len(ts.Shards)))),
	}
	if len(newSpec.Nodes) == 0 {
		return errors.New("no nodes available for new shard")
	}
	if err := m.mountShardGroup(newSpec); err != nil {
		return err
	}
	return m.meta.SplitShard(spec.Table, splitKey, []*ShardSpec{newSpec})
}

// unmountLocal closes and removes the local replica of a shard.
func (m *Manager) unmountLocal(shardID string) {
	m.mu.Lock()
	sh, ok := m.shards[shardID]
	if ok {
		delete(m.shards, shardID)
	}
	m.mu.Unlock()
	if ok {
		_ = sh.node.Close()
	}
}

// pkKeyForRowValues computes the encoded PK of a scanned row (schema order).
func pkKeyForRowValues(schema *sqlx.TableSchema, row sqlx.Row) (string, error) {
	idx := map[string]int{}
	for i, c := range schema.Columns {
		idx[c.Name] = i
	}
	pk := make([]sqlx.Value, 0, len(schema.PK))
	for _, p := range schema.PK {
		i, ok := idx[p]
		if !ok || i >= len(row) {
			return "", fmt.Errorf("pk column %s missing from row", p)
		}
		pk = append(pk, row[i])
	}
	return storage.EncodePK(pk), nil
}

// insertForRow builds an INSERT statement for a scanned row.
func insertForRow(schema *sqlx.TableSchema, row sqlx.Row) string {
	cols := make([]string, len(schema.Columns))
	vals := make([]string, len(row))
	for i, c := range schema.Columns {
		cols[i] = c.Name
		if i < len(row) {
			vals[i] = literalFor(row[i])
		} else {
			vals[i] = "NULL"
		}
	}
	return "INSERT INTO " + schema.Name + " (" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
}

func literalFor(v sqlx.Value) string {
	switch v.Type {
	case sqlx.TypeNull:
		return "NULL"
	case sqlx.TypeInt:
		return strconv.FormatInt(v.Int, 10)
	case sqlx.TypeFloat:
		return strconv.FormatFloat(v.Flt, 'g', -1, 64)
	case sqlx.TypeString:
		return "'" + strings.ReplaceAll(v.Str, "'", "''") + "'"
	case sqlx.TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case sqlx.TypeTimestamp:
		return "'" + v.Tm.UTC().Format(time.RFC3339Nano) + "'"
	}
	return "NULL"
}

func parseValueString(t sqlx.Type, s string) (sqlx.Value, error) {
	switch t {
	case sqlx.TypeInt:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sqlx.NullValue, fmt.Errorf("invalid int pk value %q", s)
		}
		return sqlx.IntValue(v), nil
	case sqlx.TypeFloat:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return sqlx.NullValue, fmt.Errorf("invalid float pk value %q", s)
		}
		return sqlx.FloatValue(v), nil
	case sqlx.TypeBool:
		return sqlx.BoolValue(s == "true" || s == "1"), nil
	default:
		return sqlx.StrValue(s), nil
	}
}

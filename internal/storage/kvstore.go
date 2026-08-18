package storage

import (
	"bytes"
	"path/filepath"
	"sort"

	"ydbgo/internal/kv"
)

// KVMVCCStore adapts internal/kv (byte MVCC store with revisions) to the
// storage engine's store interface, providing the ENGINE=KV backend. Schema
// definitions and row data live in one key space, separated by a prefix:
//
//	's' \x00 <table>                 schema definition
//	'k' \x00 <table> \x00 <key>      one raw byte-KV entry (KV surface)
//	'r' \x00 <table> \x00 <pk-bytes> one row
//
// Every commit applies the batch as one kv.Store.Apply at the next revision,
// so group-commit semantics (one store commit per raft batch) are preserved
// while the data itself gains revisions (the ENGINE=KV MVCC feature).
const (
	kvSchemaTag byte = 's'
	kvDataTag   byte = 'k'
	kvRowTag    byte = 'r'
)

func kvSchemaKey(name string) []byte {
	b := make([]byte, 0, 2+len(name))
	b = append(b, kvSchemaTag, 0x00)
	return append(b, name...)
}

func kvSchemaBounds() (lower, upper []byte) {
	return []byte{kvSchemaTag, 0x00}, []byte{kvSchemaTag, 0x01}
}

func kvRowKey(table string, pk []byte) []byte {
	b := make([]byte, 0, 3+len(table)+len(pk))
	b = append(b, kvRowTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return append(b, pk...)
}

func kvRowPrefix(table string) []byte {
	b := make([]byte, 0, 3+len(table))
	b = append(b, kvRowTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return b
}

// kvRowBounds returns the exclusive range covering every row of a table.
// The upper bound bumps the trailing 0x00 separator to 0x01; every row key
// starts with that separator (followed by the pk's type tag, 0x01..0x05), so
// all rows sort strictly below it.
func kvRowBounds(table string) (lower, upper []byte) {
	lower = kvRowPrefix(table)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

func kvDataKey(table string, key []byte) []byte {
	b := make([]byte, 0, 3+len(table)+len(key))
	b = append(b, kvDataTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return append(b, key...)
}

func kvDataPrefix(table string) []byte {
	b := make([]byte, 0, 3+len(table))
	b = append(b, kvDataTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return b
}

// kvDataBounds returns a range over the raw byte-KV area of a table,
// optionally narrowed to [start, end] in byte order.
func kvDataBounds(table string, start, end string) (lower, upper []byte) {
	base := kvDataPrefix(table)
	lower = append([]byte(nil), base...)
	upper = append([]byte(nil), base...)
	upper[len(upper)-1]++
	if start != "" {
		lower = append(base, start...)
	}
	if end != "" {
		upper = append(base, end...)
	}
	return lower, upper
}

// kvStore is the ENGINE=KV store binding.
type kvStore struct {
	st *kv.Store
}

func openKV(dir string) (*kvStore, error) {
	s, err := kv.Open(filepath.Join(dir, "engine_kv"))
	if err != nil {
		return nil, err
	}
	return &kvStore{st: s}, nil
}

func (s *kvStore) Close() error { return s.st.Close() }

// CompactLSM forces a full LSM compaction over the store's key space.
func (s *kvStore) CompactLSM() error { return s.st.CompactLSM() }

func (s *kvStore) begin() (storeTx, error) {
	return &kvTx{s: s, overlay: map[string]*pending{}}, nil
}

func (s *kvStore) view(fn func(tx storeTx) error) error {
	return fn(&kvTx{s: s})
}

// snapshot captures a point-in-time view pinned at the current committed
// revision; release it with rollback.
func (s *kvStore) snapshot() (storeTx, error) {
	return &kvTx{s: s, snap: s.st.Snapshot()}, nil
}

// pending is an uncommitted write in the overlay (read-your-writes view).
type pending struct {
	value []byte
	del   bool
}

// kvTx buffers writes in memory and flushes them as one kv.Apply on commit,
// giving the storage engine an MVCC-capable writable/revisioned tx. When snap
// is set the tx is a read-only point-in-time view (raft FSM snapshot path).
type kvTx struct {
	s       *kvStore
	overlay map[string]*pending // key -> latest pending write ('' = delete)
	ops     []kv.Op
	active  bool // writable (from begin); otherwise read-only view
	snap    *kv.Snapshot
}

func (t *kvTx) pkey(key []byte) string { return string(key) }

func (t *kvTx) put(key, value []byte) {
	k := t.pkey(key)
	t.overlay[k] = &pending{value: append([]byte(nil), value...)}
	t.ops = append(t.ops, kv.Op{Key: append([]byte(nil), key...), Value: value})
}

func (t *kvTx) del(key []byte) {
	k := t.pkey(key)
	t.overlay[k] = &pending{del: true}
	t.ops = append(t.ops, kv.Op{Key: append([]byte(nil), key...), Delete: true})
}

// get reads through the overlay (own pending writes first), then the store.
func (t *kvTx) get(key []byte) ([]byte, error) {
	if p, ok := t.overlay[t.pkey(key)]; ok {
		if p.del {
			return nil, nil
		}
		return p.value, nil
	}
	if t.snap != nil {
		v, ok, err := t.snap.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return v, nil
	}
	v, ok, err := t.s.st.Get(0, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return v, nil
}

// each merges the committed range with overlay writes in key order.
func (t *kvTx) each(lower, upper []byte, fn func(k, v []byte) error) error {
	ordered := []string{}
	vals := map[string][]byte{}
	if t.snap != nil {
		if err := t.snap.Range(lower, upper, func(key, val []byte, deleted bool) error {
			if !deleted {
				ordered = append(ordered, t.pkey(key))
				vals[t.pkey(key)] = append([]byte(nil), val...)
			}
			return nil
		}); err != nil {
			return err
		}
	} else {
		err := t.s.st.Range(0, lower, upper, func(key, val []byte, deleted bool) error {
			if !deleted {
				ordered = append(ordered, t.pkey(key))
				vals[t.pkey(key)] = append([]byte(nil), val...)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	// overlay may add or replace keys that fall in this range.
	for _, op := range t.ops {
		k := t.pkey(op.Key)
		if bytes.Compare(op.Key, lower) < 0 || bytes.Compare(op.Key, upper) >= 0 {
			continue
		}
		if op.Delete {
			if _, was := vals[k]; was {
				delete(vals, k)
			}
			continue
		}
		if _, was := vals[k]; !was {
			ordered = append(ordered, k)
		}
		vals[k] = op.Value
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		if err := fn([]byte(k), vals[k]); err != nil {
			return err
		}
	}
	return nil
}

func (t *kvTx) schemaPut(name string, def []byte) error   { t.put(kvSchemaKey(name), def); return nil }
func (t *kvTx) schemaGet(name string) ([]byte, error)     { return t.get(kvSchemaKey(name)) }
func (t *kvTx) schemaDelete(name string) error            { t.del(kvSchemaKey(name)); return nil }
func (t *kvTx) schemaNames() ([]string, error) {
	names := []string{}
	lower, upper := kvSchemaBounds()
	err := t.each(lower, upper, func(k, _ []byte) error {
		names = append(names, string(k[len(lower):]))
		return nil
	})
	return names, err
}

func (t *kvTx) rowPut(table string, key []byte, val []byte) error {
	t.put(kvRowKey(table, key), val)
	return nil
}
func (t *kvTx) rowGet(table string, key []byte) ([]byte, error) {
	return t.get(kvRowKey(table, key))
}
func (t *kvTx) rowDelete(table string, key []byte) error {
	t.del(kvRowKey(table, key))
	return nil
}
func (t *kvTx) rowEach(table string, fn func(k, v []byte) error) error {
	lower, upper := kvRowBounds(table)
	pl := len(lower)
	return t.each(lower, upper, func(k, v []byte) error {
		return fn(k[pl:], v)
	})
}
func (t *kvTx) rowDeleteAll(name string) error {
	lower, upper := kvRowBounds(name)
	var keys [][]byte
	t.each(lower, upper, func(k, _ []byte) error {
		keys = append(keys, append([]byte(nil), k...))
		return nil
	})
	for _, k := range keys {
		t.del(k)
	}
	return nil
}

// dataGet reads a raw byte-KV entry value ("" when absent).
func (t *kvTx) dataGet(table string, key string) ([]byte, error) {
	return t.get(kvDataKey(table, []byte(key)))
}

// dataDelete removes a raw byte-KV entry.
func (t *kvTx) dataDelete(table string, key string) error {
	t.del(kvDataKey(table, []byte(key)))
	return nil
}

// dataPut stores a raw byte-KV entry.
func (t *kvTx) dataPut(table string, key string, value []byte) error {
	t.put(kvDataKey(table, []byte(key)), value)
	return nil
}

// dataEach iterates raw byte-KV entries of a table in byte order, emitting the
// key with the table prefix stripped.
func (t *kvTx) dataEach(table string, start, end string, fn func(k, v []byte) error) error {
	lower, upper := kvDataBounds(table, start, end)
	pl := len(kvDataPrefix(table))
	return t.each(lower, upper, func(k, v []byte) error {
		return fn(k[pl:], v)
	})
}

// dataDeleteAll removes every raw byte-KV entry of a table.
func (t *kvTx) dataDeleteAll(name string) error {
	lower, upper := kvDataBounds(name, "", "")
	var keys [][]byte
	t.each(lower, upper, func(k, _ []byte) error {
		keys = append(keys, append([]byte(nil), k...))
		return nil
	})
	for _, k := range keys {
		t.del(k)
	}
	return nil
}

func (t *kvTx) dropTable(name string) error {
	if err := t.schemaDelete(name); err != nil {
		return err
	}
	if err := t.dataDeleteAll(name); err != nil {
		return err
	}
	return t.rowDeleteAll(name)
}

func (t *kvTx) commit() error {
	if len(t.ops) == 0 {
		return nil
	}
	if err := t.s.st.Apply(0, t.ops); err != nil {
		return err
	}
	t.ops = nil
	t.overlay = map[string]*pending{}
	return nil
}

func (t *kvTx) rollback() error {
	t.ops = nil
	t.overlay = map[string]*pending{}
	if t.snap != nil {
		t.snap.Close()
		t.snap = nil
	}
	return nil
}

package storage

import (
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Pebble-backed store. Schema definitions and row keys live in one key space,
// separated by a leading byte:
//
//	'm' \x00 <table>                 schema definition
//	'r' \x00 <table> \x00 <pk-bytes> one row
//
// Table names are SQL identifiers (no NUL), so the \x00 separators make every
// table's rows a contiguous range in Pebble's byte order, matching the former
// bucket ordering by encoded primary key.
//
// Durability belongs to the raft log, so commits use pebble.NoSync: the store
// is a state cache rebuilt from the raft log/snapshot on replay. The WAL stays
// enabled but is never synced on the hot path; a clean Close flushes it, so a
// normal restart starts from the cached state.
const (
	kvMeta byte = 'm'
	kvRow  byte = 'r'
)

func metaKey(name string) []byte {
	buf := make([]byte, 0, len(name)+2)
	buf = append(buf, kvMeta, 0)
	buf = append(buf, name...)
	return buf
}

func metaBounds() (lower, upper []byte) {
	return []byte{kvMeta, 0}, []byte{kvMeta, 1}
}

func rowKeyPrefix(table string) []byte {
	buf := make([]byte, 0, len(table)+3)
	buf = append(buf, kvRow, 0)
	buf = append(buf, table...)
	buf = append(buf, 0)
	return buf
}

// rowBounds returns the exclusive range covering every row key of a table.
// The upper bound is the row prefix with its trailing separator bumped from
// 0x00 to 0x01; because that separator byte is shared by all row keys, every
// key (whatever its payload bytes) sorts strictly below it.
func rowBounds(table string) (lower, upper []byte) {
	lower = rowKeyPrefix(table)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

func rowKey(table string, key []byte) []byte {
	buf := make([]byte, 0, len(table)+3+len(key))
	buf = append(buf, kvRow, 0)
	buf = append(buf, table...)
	buf = append(buf, 0)
	buf = append(buf, key...)
	return buf
}

// pebbleStore is the pebble-backed store.
type pebbleStore struct {
	db   *pebble.DB
	once sync.Once
}

func openPebble(dir string) (*pebbleStore, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	db, err := pebble.Open(filepath.Join(dir, "ydb.pebble"), opts)
	if err != nil {
		return nil, err
	}
	return &pebbleStore{db: db}, nil
}

// Close releases the DB. It is idempotent so repeated closes (e.g. an explicit
// Close followed by a deferred one) are safe.
func (s *pebbleStore) Close() error {
	var err error
	s.once.Do(func() {
		err = s.db.Close()
	})
	return err
}

// CompactLSM forces a full Pebble LSM compaction over the whole key space.
func (s *pebbleStore) CompactLSM() error {
	it, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer it.Close()
	if !it.First() {
		return nil // empty store: nothing to compact
	}
	lower := append([]byte(nil), it.Key()...)
	it.Last()
	last := append([]byte(nil), it.Key()...)
	upper := append(last, 0x00)
	return s.db.Compact(lower, upper, true)
}

func (s *pebbleStore) begin() (storeTx, error) {
	return &pebbleTx{b: s.db.NewIndexedBatch()}, nil
}

func (s *pebbleStore) view(fn func(tx storeTx) error) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	return fn(&pebbleTx{r: snap})
}

func (s *pebbleStore) snapshot() (storeTx, error) {
	return &pebbleTx{r: s.db.NewSnapshot()}, nil
}

// pebbleTx wraps either a writable Batch (from begin) or a read-only Snapshot
// (from view) as a storeTx.
type pebbleTx struct {
	b *pebble.Batch
	r *pebble.Snapshot
}

func (t *pebbleTx) writable() error {
	if t.b == nil {
		return errors.New("pebble: read-only transaction")
	}
	return nil
}

func (t *pebbleTx) commit() error {
	if t.b == nil {
		return errors.New("pebble: nothing to commit")
	}
	err := t.b.Commit(pebble.NoSync)
	t.b = nil
	return err
}

func (t *pebbleTx) rollback() error {
	if t.b != nil {
		t.b.Close()
		t.b = nil
	}
	if t.r != nil {
		t.r.Close()
		t.r = nil
	}
	return nil
}

func (t *pebbleTx) get(key []byte) ([]byte, error) {
	var (
		v      []byte
		closer io.Closer
		err    error
	)
	if t.b != nil {
		v, closer, err = t.b.Get(key)
	} else {
		v, closer, err = t.r.Get(key)
	}
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, nil
}

func (t *pebbleTx) put(key, value []byte) error {
	if err := t.writable(); err != nil {
		return err
	}
	return t.b.Set(key, value, nil)
}

func (t *pebbleTx) delete(key []byte) error {
	if err := t.writable(); err != nil {
		return err
	}
	return t.b.Delete(key, nil)
}

func (t *pebbleTx) newIter(lower, upper []byte) (*pebble.Iterator, error) {
	o := &pebble.IterOptions{LowerBound: lower, UpperBound: upper}
	if t.b != nil {
		return t.b.NewIter(o)
	}
	return t.r.NewIter(o)
}

func (t *pebbleTx) schemaPut(name string, def []byte) error {
	return t.put(metaKey(name), def)
}

func (t *pebbleTx) schemaGet(name string) ([]byte, error) {
	return t.get(metaKey(name))
}

func (t *pebbleTx) schemaDelete(name string) error {
	return t.delete(metaKey(name))
}

func (t *pebbleTx) schemaNames() ([]string, error) {
	names := []string{}
	lower, upper := metaBounds()
	iter, err := t.newIter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	pl := len(lower)
	for iter.First(); iter.Valid(); iter.Next() {
		names = append(names, string(iter.Key()[pl:]))
	}
	return names, iter.Error()
}

func (t *pebbleTx) rowPut(table string, key []byte, val []byte) error {
	return t.put(rowKey(table, key), val)
}

func (t *pebbleTx) rowGet(table string, key []byte) ([]byte, error) {
	return t.get(rowKey(table, key))
}

func (t *pebbleTx) rowDelete(table string, key []byte) error {
	return t.delete(rowKey(table, key))
}

func (t *pebbleTx) rowEach(table string, fn func(k, v []byte) error) error {
	lower, upper := rowBounds(table)
	iter, err := t.newIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	pl := len(lower)
	for iter.First(); iter.Valid(); iter.Next() {
		v, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		if err := fn(iter.Key()[pl:], v); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (t *pebbleTx) rowDeleteAll(table string) error {
	if err := t.writable(); err != nil {
		return err
	}
	lower, upper := rowBounds(table)
	return t.b.DeleteRange(lower, upper, nil)
}

func (t *pebbleTx) dropTable(name string) error {
	if err := t.schemaDelete(name); err != nil {
		return err
	}
	return t.rowDeleteAll(name)
}

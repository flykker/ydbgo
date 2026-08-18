package storage

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Engine is a storage engine backed by pluggable durable KV stores. Schema
// definitions live in memory (for fast GetSchema/validation) and durably in
// the default (TABLE) store; row data lives in a per-engine store, addressed
// by the encoded, sortable primary key. The default store is pebble;
// ENGINE=KV tables use the MVCC byte store (internal/kv); CSTORE is planned.
type Engine struct {
	mu     sync.RWMutex
	dir    string
	stores map[string]store // engine name -> backend; "" = default
	tables map[string]*table
	// active points at the current group-commit write tx map while a batch is
	// running (nil otherwise). It is stored/cleared only while holding mu.Lock
	// (inside a batch), but readers check it lock-free via the atomic pointer
	// on every read/write op: reading the flag must not block, because the
	// batch holds mu.Lock and may itself re-enter the engine through SQL.
	active atomic.Pointer[map[store]storeTx]
	noSync bool // retained for API compatibility; stores skip fsync
}

// Open opens (or creates) a database at dir.
func Open(dir string) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	st, err := newStore(dir)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		dir:    dir,
		stores: map[string]store{"": st},
		tables: map[string]*table{},
	}
	if err := e.loadTables(); err != nil {
		st.Close()
		return nil, err
	}
	// Open backends for engines referenced by persisted tables.
	for _, t := range e.tables {
		if _, err := e.engineStore(t.engine); err != nil {
			st.Close()
			return nil, err
		}
	}
	// Rebuild secondary index entries from the persisted rows (index entries
	// are a derived in-memory structure).
	if err := e.rebuildIndexes(); err != nil {
		st.Close()
		return nil, err
	}
	return e, nil
}

// loadTables rebuilds the in-memory schema index from the default store.
func (e *Engine) loadTables() error {
	return e.readFrom(e.store(""), func(tx storeTx) error {
		names, err := tx.schemaNames()
		if err != nil {
			return err
		}
		for _, name := range names {
			def, err := tx.schemaGet(name)
			if err != nil {
				return err
			}
			t, err := e.unmarshalCreateTable(def)
			if err != nil {
				return err
			}
			e.tables[name] = t
		}
		return nil
	})
}

// Close releases all stores.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var firstErr error
	for _, st := range e.stores {
		if err := st.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// lsmCompactor is implemented by stores whose backing LSM can be forced
// through a full compaction.
type lsmCompactor interface {
	CompactLSM() error
}

// CompactLSM forces a full Pebble LSM compaction on every open store,
// collapsing the freshly-written L0 SSTable runs after a bulk load. Reads then
// hit a handful of merged tables instead of the short per-batch ones. Blocking
// and expensive: for administrative use only (ADMIN COMPACT).
func (e *Engine) CompactLSM() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var firstErr error
	for _, st := range e.stores {
		c, ok := st.(lsmCompactor)
		if !ok {
			continue
		}
		if err := c.CompactLSM(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// store returns the backend for an engine name, or "" for the default.
func (e *Engine) store(engine string) store {
	if engine == "" || engine == "TABLE" {
		return e.stores[""]
	}
	return e.stores[engine]
}

// engineStore returns the backend for an engine, opening it lazily if needed.
// "" and "TABLE" map to the default pebble store.
func (e *Engine) engineStore(engine string) (store, error) {
	if engine == "" || engine == "TABLE" {
		return e.stores[""], nil
	}
	if s, ok := e.stores[engine]; ok {
		return s, nil
	}
	s, err := newStoreFor(e.dir, engine)
	if err != nil {
		return nil, err
	}
	e.stores[engine] = s
	return s, nil
}

// SetNoSync controls whether the durable store is allowed to skip the fsync
// on each commit. In raft-backed mode the raft log owns durability (raftsvc's
// fileLogStore fsyncs every batch), so the engine's per-batch store fsync is
// redundant. All backends commit with NoSync (they are a state cache; the WAL
// is only synced at Close), so this is a no-op kept for API compatibility.
func (e *Engine) SetNoSync(v bool) { _ = v }

// UpdateBatch runs fn within a single durable store transaction. All writes
// performed by fn (across every engine store they touch) share one commit; a
// failure rolls the whole batch back. The raft FSM uses this so a
// group-committed raft entry costs one storage commit per touched engine.
func (e *Engine) UpdateBatch(fn func() error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.updateBatchLocked(fn)
}

func (e *Engine) updateBatchLocked(fn func() error) error {
	txs := map[store]storeTx{}
	e.active.Store(&txs)
	defer func() { e.active.Store(nil) }()
	cerr := fn()
	if cerr != nil {
		for _, tx := range txs {
			tx.rollback()
		}
		return cerr
	}
	var firstErr error
	for _, tx := range txs {
		if err := tx.commit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// txActive reports whether the engine is inside a batch transaction.
func (e *Engine) txActive() bool { return e.active.Load() != nil }

// writeLock returns an unlock func, acquiring the write lock unless the
// caller is already inside a group-commit batch (which holds the lock).
func (e *Engine) writeLock() func() {
	if e.active.Load() != nil {
		return func() {}
	}
	e.mu.Lock()
	return e.mu.Unlock
}

// readLock returns an unlock func, acquiring the read lock unless the caller
// is already inside a group-commit batch (which holds the write lock).
func (e *Engine) readLock() func() {
	if e.active.Load() != nil {
		return func() {}
	}
	e.mu.RLock()
	return e.mu.RUnlock
}

// txFor returns the active batch transaction for a store, opening it lazily.
// Caller must hold e.mu.Lock and be inside a batch.
func (e *Engine) txFor(st store) storeTx {
	txs := e.active.Load()
	if txs == nil {
		// begin is infallible for our backends; fall back to a committed view
		return nil
	}
	if tx, ok := (*txs)[st]; ok {
		return tx
	}
	tx, err := st.begin()
	if err != nil {
		// begin is infallible for our backends; fall back to a committed view
		return nil
	}
	(*txs)[st] = tx
	return tx
}

// writeTo routes a mutation record to a specific store, using the active
// group-commit transaction when present (see txFor), or opening (and
// committing) its own otherwise. Caller must hold e.mu.Lock.
func (e *Engine) writeTo(st store, fn func(tx storeTx) error) error {
	if e.active.Load() != nil {
		return fn(e.txFor(st))
	}
	tx, err := st.begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.rollback()
		return err
	}
	return tx.commit()
}

// readFrom routes a read through the active batch transaction of the store
// (so a batch sees its own uncommitted writes) or a read-only snapshot.
// Caller holds e.mu.
func (e *Engine) readFrom(st store, fn func(tx storeTx) error) error {
	if e.active.Load() != nil {
		if tx := e.txFor(st); tx != nil {
			return fn(tx)
		}
		return st.view(fn)
	}
	return st.view(fn)
}

// write routes a mutation record to the default store. Caller holds e.mu.
func (e *Engine) write(fn func(tx storeTx) error) error {
	return e.writeTo(e.store(""), fn)
}

// read routes a read through the default store. Caller holds e.mu.
func (e *Engine) read(fn func(tx storeTx) error) error {
	return e.readFrom(e.store(""), fn)
}

// EstimateSize returns a rough byte estimate of all stored row data.
func (e *Engine) EstimateSize() uint64 {
	var n uint64
	for _, name := range e.SortedTables() {
		t := e.tables[name]
		st := e.store(t.engine)
		st.view(func(tx storeTx) error {
			_ = tx.rowEach(name, func(k, v []byte) error {
				n += uint64(len(k)) + uint64(len(v)) + 24
				return nil
			})
			return nil
		})
	}
	return n
}

func (e *Engine) SortedTables() []string {
	names := make([]string, 0, len(e.tables))
	for n := range e.tables {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

type table struct {
	name      string
	cols      []colInfo
	pk        []int // column indexes forming PK
	indexes   map[string]*index
	engine    string        // storage engine: "TABLE" (default), "KV", "CSTORE"
	retention time.Duration // auto-delete rows older than now-retention (0 = disabled)
}

func (t *table) Name() string { return t.name }

type colInfo struct {
	name    string
	typ     sqlType
	notNull bool
}

type index struct {
	name    string
	cols    []int
	entries map[string][]string // indexKey -> list of pkKeys
}

type sqlType int

const (
	tInt sqlType = iota
	tFloat
	tString
	tBool
	tTimestamp
)

func (t sqlType) String() string {
	switch t {
	case tInt:
		return "int64"
	case tFloat:
		return "float64"
	case tString:
		return "string"
	case tBool:
		return "bool"
	case tTimestamp:
		return "timestamp"
	}
	return "null"
}

type sqlValue struct {
	typ  sqlType
	null bool
	i    int64
	f    float64
	s    string
	b    bool
}

// notFoundError signals missing table.
type notFoundError struct{ table string }

func (e notFoundError) Error() string  { return fmt.Sprintf("table %q not found", e.table) }
func (e notFoundError) NotFound() bool { return true }

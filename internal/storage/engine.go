package storage

import (
	"fmt"
	"os"
	"sync"
)

// Engine is a storage engine backed by a pluggable durable KV store (bbolt by
// default). Schema definitions live in memory (for fast GetSchema/validation)
// and durably in the store; row data lives in the store, addressed by the
// encoded, sortable primary key.
type Engine struct {
	mu     sync.RWMutex
	dir    string
	store  store
	tables map[string]*table
	active storeTx // current group-commit write transaction (nil when none)
	noSync bool    // retained for API compatibility; bbolt always syncs on commit
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
		store:  st,
		tables: map[string]*table{},
	}
	if err := e.loadTables(); err != nil {
		st.Close()
		return nil, err
	}
	return e, nil
}

// loadTables rebuilds the in-memory schema index from the store.
func (e *Engine) loadTables() error {
	return e.store.view(func(tx storeTx) error {
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

// Close releases the store.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.Close()
}

// SetNoSync is retained for API compatibility. With bbolt the engine always
// syncs on commit; the raft log remains the additional durability source.
func (e *Engine) SetNoSync(v bool) {}

// UpdateBatch runs fn within a single durable store transaction. All writes
// performed by fn share one fsync at commit; a failure rolls the whole batch
// back. The raft FSM uses this so a group-committed raft entry costs one
// storage commit (group commit at the storage layer).
func (e *Engine) UpdateBatch(fn func() error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.updateBatchLocked(fn)
}

func (e *Engine) updateBatchLocked(fn func() error) error {
	tx, err := e.store.begin()
	if err != nil {
		return err
	}
	e.active = tx
	cerr := fn()
	e.active = nil
	if cerr != nil {
		tx.rollback()
		return cerr
	}
	return tx.commit()
}

// txActive reports whether the engine is inside a batch transaction.
func (e *Engine) txActive() bool { return e.active != nil }

// writeLock returns an unlock func, acquiring the write lock unless the
// caller is already inside a group-commit batch (which holds the lock).
func (e *Engine) writeLock() func() {
	if e.active != nil {
		return func() {}
	}
	e.mu.Lock()
	return e.mu.Unlock
}

// readLock returns an unlock func, acquiring the read lock unless the caller
// is already inside a group-commit batch (which holds the write lock).
func (e *Engine) readLock() func() {
	if e.active != nil {
		return func() {}
	}
	e.mu.RLock()
	return e.mu.RUnlock
}

// write routes a mutation record to the store, using the active group-commit
// transaction when present, or opening (and committing) its own otherwise.
// Caller must hold e.mu.Lock.
func (e *Engine) write(fn func(tx storeTx) error) error {
	if e.active != nil {
		return fn(e.active)
	}
	tx, err := e.store.begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.rollback()
		return err
	}
	return tx.commit()
}

// read routes a read through the active batch transaction (so a batch sees
// its own uncommitted writes) or a read-only snapshot. Caller holds e.mu.
func (e *Engine) read(fn func(tx storeTx) error) error {
	if e.active != nil {
		return fn(e.active)
	}
	return e.store.view(fn)
}

// EstimateSize returns a rough byte estimate of all stored row data.
func (e *Engine) EstimateSize() uint64 {
	var n uint64
	e.store.view(func(tx storeTx) error {
		names, _ := tx.schemaNames()
		for _, name := range names {
			_ = tx.rowEach(name, func(k, v []byte) error {
				n += uint64(len(k)) + uint64(len(v)) + 24
				return nil
			})
		}
		return nil
	})
	return n
}

type table struct {
	name    string
	cols    []colInfo
	pk      []int // column indexes forming PK
	indexes map[string]*index
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

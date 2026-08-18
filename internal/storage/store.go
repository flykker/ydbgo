package storage

// store is the pluggable durable backend of the Engine. It holds the row
// data (and per-table schema definitions) as an embedded KV store. Any KV
// engine implementing this interface can back the Engine; pebble provides the
// default implementation and internal/kv the ENGINE=KV (MVCC) implementation.
type store interface {
	// Close releases the backend.
	Close() error
	// begin opens a read-write transaction. All statements of a group-commit
	// batch go into one transaction and commit together without an fsync:
	// durability belongs to the raft log, so the store is a state cache.
	begin() (storeTx, error)
	// view runs fn inside a read-only snapshot transaction.
	view(fn func(tx storeTx) error) error
	// snapshot captures a point-in-time read-only transaction that stays valid
	// after the call returns (unlike view). The caller must release it via
	// rollback when done. Used by the raft FSM snapshot path so serialization
	// can run on the snapshot writer goroutine without blocking applies.
	snapshot() (storeTx, error)
}

// storeTx is a transaction-scoped handle over the backend.
type storeTx interface {
	// schema definitions, keyed by table name.
	schemaPut(name string, def []byte) error
	schemaGet(name string) ([]byte, error)
	schemaDelete(name string) error
	schemaNames() ([]string, error)
	// rows, keyed by the encoded (sortable) primary key.
	rowPut(table string, key []byte, val []byte) error
	rowGet(table string, key []byte) ([]byte, error)
	rowDelete(table string, key []byte) error
	rowEach(table string, fn func(k, v []byte) error) error
	// rowDeleteAll removes every row of a table (keeps the schema).
	rowDeleteAll(table string) error
	// dropTable removes a table's schema and all of its rows atomically.
	dropTable(name string) error
	// commit publishes the transaction's writes to the store (no fsync; the
	// raft log owns durability).
	commit() error
	// rollback abandons the transaction.
	rollback() error
}

// newStoreFor opens a durable backend for the given storage engine. "" and
// "TABLE" use pebble; "KV" uses the MVCC byte store (internal/kv); "CSTORE"
// uses the columnar MVCC store (internal/kv with column-major layout).
func newStoreFor(dir, engine string) (store, error) {
	switch engine {
	case "", "TABLE":
		return openPebble(dir)
	case "KV":
		return openKV(dir)
	case "CSTORE":
		return openCStore(dir)
	case "CSTORE2":
		return openMpart(dir)
	}
	return nil, &unknownEngineError{engine: engine}
}

// newStore opens the default (TABLE) backend.
func newStore(dir string) (store, error) {
	return newStoreFor(dir, "")
}

// unknownEngineError signals an unrecognized ENGINE= name.
type unknownEngineError struct{ engine string }

func (e *unknownEngineError) Error() string {
	return "storage: unknown engine " + e.engine
}

package storage

import (
	"path/filepath"
)

// store is the pluggable durable backend of the Engine. It holds the row
// data (and per-table schema definitions) as an embedded KV store. Any KV
// engine implementing this interface can back the Engine; bbolt provides the
// default implementation.
type store interface {
	// Close releases the backend.
	Close() error
	// begin opens a durable read-write transaction. A single fsync happens at
	// commit; batching all of a group-commit's statements into one transaction
	// therefore yields one fsync per batch.
	begin() (storeTx, error)
	// view runs fn inside a read-only snapshot transaction.
	view(fn func(tx storeTx) error) error
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
	// dropTable removes a table's schema and all of its rows atomically.
	dropTable(name string) error
	// commit persists the transaction to disk (fsync).
	commit() error
	// rollback abandons the transaction.
	rollback() error
}

func newStore(dir string) (store, error) {
	return openBbolt(filepath.Join(dir, "ydb.bbolt"))
}

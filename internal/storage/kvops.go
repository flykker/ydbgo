package storage

import (
	"errors"

	sqlx "ydbgo/internal/sql"
)

var _ sqlx.KVEngine = (*Engine)(nil)

// kvtx is the narrow interface a store transaction needs to satisfy to back
// the raw byte-KV surface (implemented by *kvTx).
type kvtx interface {
	dataGet(table, key string) ([]byte, error)
	dataPut(table, key string, value []byte) error
	dataDelete(table, key string) error
	dataEach(table, start, end string, fn func(k, v []byte) error) error
}

// KVPut implements sqlx.KVEngine: stores a raw byte key/value pair in the
// ENGINE=KV area of the table.
func (e *Engine) KVPut(table, key, value string) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return err
	}
	if t.engine != "KV" {
		return errors.New("table " + table + " is not an ENGINE=KV table")
	}
	return e.writeTo(e.store(t.engine), func(tx storeTx) error {
		kt, ok := tx.(*kvTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the KV store")
		}
		return kt.dataPut(table, key, []byte(value))
	})
}

// KVGet implements sqlx.KVEngine: reads a raw byte key's value. A missing key
// returns a Null value.
func (e *Engine) KVGet(table, key string) (sqlx.Value, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return sqlx.NullValue, err
	}
	if t.engine != "KV" {
		return sqlx.NullValue, errors.New("table " + table + " is not an ENGINE=KV table")
	}
	var out []byte
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		kt, ok := tx.(*kvTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the KV store")
		}
		v, err := kt.dataGet(table, key)
		if err != nil {
			return err
		}
		out = append(out, v...)
		return nil
	})
	if err != nil {
		return sqlx.NullValue, err
	}
	if len(out) == 0 {
		return sqlx.NullValue, nil
	}
	return sqlx.StrValue(string(out)), nil
}

// KVDelete implements sqlx.KVEngine: removes a raw byte key.
func (e *Engine) KVDelete(table, key string) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return err
	}
	if t.engine != "KV" {
		return errors.New("table " + table + " is not an ENGINE=KV table")
	}
	return e.writeTo(e.store(t.engine), func(tx storeTx) error {
		kt, ok := tx.(*kvTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the KV store")
		}
		return kt.dataDelete(table, key)
	})
}

// KVScan implements sqlx.KVEngine: iterates the raw byte-KV entries of a table
// in byte order, optionally narrowed to [start, end].
func (e *Engine) KVScan(table, start, end string) ([]sqlx.KVEntry, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "KV" {
		return nil, errors.New("table " + table + " is not an ENGINE=KV table")
	}
	var out []sqlx.KVEntry
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		kt, ok := tx.(*kvTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the KV store")
		}
		return kt.dataEach(table, start, end, func(k, v []byte) error {
			out = append(out, sqlx.KVEntry{Key: string(k), Value: string(v)})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

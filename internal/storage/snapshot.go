package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// frameRecord wraps a payload in the WAL record framing (len][crc][payload).
func frameRecord(payload []byte) []byte {
	buf := make([]byte, walHeader+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)
	return buf
}

const walHeader = 8

// MarshalState serializes the entire engine state (all tables, schemas and
// rows) as a framed stream of WAL records. The output is directly replayable
// via applyFramed and is used as the raft FSM snapshot payload. Rows are read
// from each table's own engine store.
func (e *Engine) MarshalState() ([]byte, error) {
	var out []byte
	for _, name := range e.sortedTables() {
		t := e.tables[name]
		out = append(out, frameRecord(e.encodeCreateTable(t))...)
		err := e.readFrom(e.store(t.engine), func(tx storeTx) error {
			return tx.rowEach(name, func(k, v []byte) error {
				vals, err := decodeRow(v, t)
				if err != nil {
					return err
				}
				out = append(out, frameRecord(e.encodeMutate(name, mutatePut{key: string(k), values: vals}))...)
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
		if t.engine == "KV" {
			err := e.readFrom(e.store(t.engine), func(tx storeTx) error {
				kt, ok := tx.(*kvTx)
				if !ok {
					return nil
				}
				return kt.dataEach(name, "", "", func(k, v []byte) error {
					out = append(out, frameRecord(e.encodeKVData(name, k, v))...)
					return nil
				})
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// ReplaceState rebuilds the engine from a MarshalState payload, atomically
// dropping any existing state (schemas from the default store, rows from each
// engine store) and installing the snapshot.
func (e *Engine) ReplaceState(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	txs := map[store]storeTx{}
	e.active.Store(&txs)
	defer func() { e.active.Store(nil) }()
	for _, name := range e.sortedTables() {
		t := e.tables[name]
		if err := e.writeTo(e.store(""), func(tx storeTx) error {
			return tx.schemaDelete(name)
		}); err != nil {
			return err
		}
		if err := e.writeTo(e.store(t.engine), func(tx storeTx) error {
			return tx.rowDeleteAll(name)
		}); err != nil {
			return err
		}
	}
	e.tables = map[string]*table{}
	err := e.applyFramed(data)
	if err != nil {
		for _, tx := range txs {
			tx.rollback()
		}
		return err
	}
	var firstErr error
	for _, tx := range txs {
		if err := tx.commit(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// applyFramed decodes a stream of framed WAL records and applies each.
func (e *Engine) applyFramed(data []byte) error {
	for pos := 0; pos+walHeader <= len(data); {
		ln := binary.LittleEndian.Uint32(data[pos : pos+4])
		crc := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
		pos += walHeader
		if ln == 0 || pos+int(ln) > len(data) {
			break
		}
		payload := data[pos : pos+int(ln)]
		pos += int(ln)
		if crc32.ChecksumIEEE(payload) != crc {
			return errors.New("snapshot: crc mismatch")
		}
		if err := e.applyRecord(payload); err != nil {
			return err
		}
	}
	return nil
}

// applyRecord applies a single mutation/create/drop record to the stores and
// the in-memory schema, using the active batch transaction when one is present.
func (e *Engine) applyRecord(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	switch payload[0] {
	case recCreateTable:
		t, err := e.unmarshalCreateTable(payload)
		if err != nil {
			return err
		}
		if _, err := e.engineStore(t.engine); err != nil {
			return err
		}
		def := e.encodeCreateTable(t)
		if err := e.write(func(tx storeTx) error {
			return tx.schemaPut(t.name, def)
		}); err != nil {
			return err
		}
		e.tables[t.name] = t
	case recDropTable:
		name, err := e.unmarshalDropTable(payload)
		if err != nil {
			return err
		}
		if t, ok := e.tables[name]; ok {
			if err := e.write(func(tx storeTx) error {
				return tx.schemaDelete(name)
			}); err != nil {
				return err
			}
			if err := e.writeTo(e.store(t.engine), func(tx storeTx) error {
				if kt, ok := tx.(*kvTx); ok {
					if err := kt.dataDeleteAll(name); err != nil {
						return err
					}
				}
				return tx.rowDeleteAll(name)
			}); err != nil {
				return err
			}
			delete(e.tables, name)
		}
	case recMutate:
		table, put, isPut, err := e.unmarshalMutate(payload)
		if err != nil {
			return err
		}
		t, ok := e.tables[table]
		if !ok {
			return nil
		}
		return e.writeTo(e.store(t.engine), func(tx storeTx) error {
			if isPut {
				return tx.rowPut(t.name, []byte(put.key), encodeRow(put.values, t))
			}
			return tx.rowDelete(t.name, []byte(put.key))
		})
	case recKVData:
		table, key, value, err := e.unmarshalKVData(payload)
		if err != nil {
			return err
		}
		t, ok := e.tables[table]
		if !ok || t.engine != "KV" {
			return nil
		}
		return e.writeTo(e.store(t.engine), func(tx storeTx) error {
			kt, ok := tx.(*kvTx)
			if !ok {
				return nil
			}
			return kt.dataPut(table, key, []byte(value))
		})
	}
	return nil
}

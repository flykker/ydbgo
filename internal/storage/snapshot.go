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
// via applyFramed and is used as the raft FSM snapshot payload.
func (e *Engine) MarshalState() ([]byte, error) {
	var out []byte
	e.store.view(func(tx storeTx) error {
		names, err := tx.schemaNames()
		if err != nil {
			return err
		}
		for _, name := range names {
			t := e.tables[name]
			out = append(out, frameRecord(e.encodeCreateTable(t))...)
			err := tx.rowEach(name, func(k, v []byte) error {
				vals, err := decodeRow(v, t)
				if err != nil {
					return err
				}
				out = append(out, frameRecord(e.encodeMutate(name, mutatePut{key: string(k), values: vals}))...)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return out, nil
}

// ReplaceState rebuilds the engine from a MarshalState payload, atomically
// dropping any existing state and installing the snapshot in the store.
func (e *Engine) ReplaceState(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	tx, err := e.store.begin()
	if err != nil {
		return err
	}
	e.active = tx
	defer func() { e.active = nil }()
	names, _ := tx.schemaNames()
	for _, name := range names {
		if err := tx.dropTable(name); err != nil {
			tx.rollback()
			return err
		}
	}
	e.tables = map[string]*table{}
	err = e.applyFramed(data)
	if err != nil {
		tx.rollback()
		return err
	}
	return tx.commit()
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

// applyRecord applies a single mutation/create/drop record to the store and
// the in-memory schema, using the active transaction when one is present.
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
		if err := e.write(func(tx storeTx) error {
			return tx.dropTable(name)
		}); err != nil {
			return err
		}
		delete(e.tables, name)
	case recMutate:
		table, put, isPut, err := e.unmarshalMutate(payload)
		if err != nil {
			return err
		}
		t, ok := e.tables[table]
		if !ok {
			return nil
		}
		return e.write(func(tx storeTx) error {
			if isPut {
				return tx.rowPut(t.name, []byte(put.key), encodeRow(put.values, t))
			}
			return tx.rowDelete(t.name, []byte(put.key))
		})
	}
	return nil
}

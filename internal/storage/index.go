package storage

import (
	"encoding/binary"
	"fmt"
	"strings"

	sqlx "ydbgo/internal/sql"
)

var _ sqlx.IndexEngine = (*Engine)(nil)

// Secondary indexes are derived structures: only their definition is
// persisted (in the create-table schema record); the entries are kept in
// memory and rebuilt from rows on open/snapshot-restore and maintained
// incrementally by every DML write. Index columns must be non-null to be
// indexed (null values are skipped).
//
// Entry key: the encoded index value (indexVal). Each index keeps a map of
// encoded value -> pks (the encoded primary keys of the matching rows), so
// an equality lookup is a single map hit and a leading-literal LIKE match is
// a prefix scan over the few distinct values.

// indexVal encodes one index column value in a self-delimiting, order-
// preserving form. Strings end with a 0x00 terminator; numerics are
// fixed-width, so concatenations for composite indexes are unambiguous.
func indexVal(v sqlValue) []byte {
	switch v.typ {
	case tInt, tTimestamp:
		var b [9]byte
		b[0] = 0x00
		binary.BigEndian.PutUint64(b[1:], uint64(v.i)+(1<<63))
		return b[:]
	case tFloat:
		var b [9]byte
		b[0] = 0x01
		binary.BigEndian.PutUint64(b[1:], mathFloatToSortable(v.f))
		return b[:]
	case tString:
		b := make([]byte, 0, len(v.s)+2)
		b = append(b, 0x02)
		b = append(b, v.s...)
		return append(b, 0x00)
	case tBool:
		if v.b {
			return []byte{0x03, 0x01}
		}
		return []byte{0x03, 0x00}
	}
	return nil
}

// indexValForCols encodes the indexed columns of a row in order, or nil when
// any indexed column is missing or null (nulls are not indexed).
func indexValForCols(vals map[string]sqlValue, t *table, ix *index) []byte {
	var out []byte
	for _, c := range ix.cols {
		v, ok := vals[t.cols[c].name]
		if !ok || v.null {
			return nil
		}
		out = append(out, indexVal(v)...)
	}
	return out
}

// indexStringPrefix returns the encoding prefix shared by every string value
// whose raw bytes begin with s (used for leading-literal LIKE lookups).
func indexStringPrefix(s string) []byte {
	b := make([]byte, 0, len(s)+1)
	b = append(b, 0x02)
	return append(b, s...)
}

// indexExistsError signals a duplicate index.
type indexExistsError struct{ name, table string }

func (e *indexExistsError) Error() string {
	return "index " + e.name + " already exists on table " + e.table
}

// CreateIndex implements sqlx.IndexEngine: builds a secondary index on a
// table, backfilling entries from existing rows and persisting the index
// definition in the table's schema.
func (e *Engine) CreateIndex(table, name string, cols []string, ifNotExists bool) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return err
	}
	if _, ok := t.indexes[name]; ok {
		if ifNotExists {
			return nil
		}
		return &indexExistsError{name: name, table: table}
	}
	if len(cols) == 0 {
		return fmt.Errorf("index %s needs at least one column", name)
	}
	if len(cols) > 1 {
		return fmt.Errorf("index %s: composite indexes are not supported yet (use a single column)", name)
	}
	colIdx := make([]int, len(cols))
	for i, c := range cols {
		found := -1
		for j, cc := range t.cols {
			if cc.name == c {
				found = j
				break
			}
		}
		if found < 0 {
			return fmt.Errorf("index column %q not found in table %s", c, table)
		}
		colIdx[i] = found
	}
	ix := &index{name: name, cols: colIdx, entries: map[string][]string{}}
	t.indexes[name] = ix
	// Backfill from existing rows, then persist the updated schema so the
	// definition survives a restart (entries are rebuilt on open).
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		return tx.rowEach(table, func(k, v []byte) error {
			vals, err := decodeRow(v, t)
			if err != nil {
				return err
			}
			enc := indexValForCols(vals, t, ix)
			if enc != nil {
				ix.entries[string(enc)] = append(ix.entries[string(enc)], string(k))
			}
			return nil
		})
	})
	if err != nil {
		delete(t.indexes, name)
		return err
	}
	if err := e.write(func(tx storeTx) error {
		return tx.schemaPut(table, e.encodeCreateTable(t))
	}); err != nil {
		delete(t.indexes, name)
		return err
	}
	return nil
}

// DropIndex implements sqlx.IndexEngine: removes an index definition and its
// in-memory entries.
func (e *Engine) DropIndex(table, name string, ifExists bool) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return err
	}
	if _, ok := t.indexes[name]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("index %s not found on table %s", name, table)
	}
	delete(t.indexes, name)
	return e.write(func(tx storeTx) error {
		return tx.schemaPut(table, e.encodeCreateTable(t))
	})
}

// rebuildIndexes rebuilds every secondary index's entries from the table's
// rows. Called on open and after a snapshot restore; DML afterwards keeps the
// entries in sync.
func (e *Engine) rebuildIndexes() error {
	for _, name := range e.SortedTables() {
		t := e.tables[name]
		if len(t.indexes) == 0 {
			continue
		}
		for _, ix := range t.indexes {
			ix.entries = map[string][]string{}
		}
		if err := e.readFrom(e.store(t.engine), func(tx storeTx) error {
			return tx.rowEach(name, func(k, v []byte) error {
				vals, err := decodeRow(v, t)
				if err != nil {
					return err
				}
				addRowIndexes(t, vals, string(k))
				return nil
			})
		}); err != nil {
			return err
		}
	}
	return nil
}

// addRowIndexes records a row's pk under each index's encoded value.
func addRowIndexes(t *table, vals map[string]sqlValue, pk string) {
	for _, ix := range t.indexes {
		enc := indexValForCols(vals, t, ix)
		if enc == nil {
			continue
		}
		ix.entries[string(enc)] = append(ix.entries[string(enc)], pk)
	}
}

// removeRowIndexes drops a row's pk from every index it was recorded under.
func removeRowIndexes(t *table, vals map[string]sqlValue, pk string) {
	for _, ix := range t.indexes {
		enc := indexValForCols(vals, t, ix)
		if enc == nil {
			continue
		}
		pks := ix.entries[string(enc)]
		for i, p := range pks {
			if p == pk {
				pks[i] = pks[len(pks)-1]
				pks = pks[:len(pks)-1]
				break
			}
		}
		if len(pks) == 0 {
			delete(ix.entries, string(enc))
		} else {
			ix.entries[string(enc)] = pks
		}
	}
}

// indexForCol returns an index whose first indexed column is col (nil when
// the table has no such index).
func (t *table) indexForCol(col int) *index {
	for _, ix := range t.indexes {
		if len(ix.cols) > 0 && ix.cols[0] == col {
			return ix
		}
	}
	return nil
}

// inPKRange reports whether an encoded pk falls inside [plLower, plUpper)
// (nil bound = unbounded on that side).
func inPKRange(pk []byte, plLower, plUpper []byte) bool {
	if len(plLower) > 0 && string(pk) < string(plLower) {
		return false
	}
	if len(plUpper) > 0 && string(pk) >= string(plUpper) {
		return false
	}
	return true
}

// indexMatchRange iterates every pk that the secondary index on pred.Col
// matches for pred, skipping pks outside the PK window. It returns ok=false
// when no index can serve the predicate (the caller falls back to a columnar
// scan). The entries are derived from the same rows a scan would evaluate, so
// the matched set is identical to matchesFilter's.
func (e *Engine) indexMatchRange(t *table, pred *sqlx.ColumnFilter, plLower, plUpper []byte, fn func(pk []byte) error) (bool, error) {
	ix := t.indexForCol(pred.Col)
	if ix == nil || pred.Lit.Null {
		return false, nil
	}
	col := t.cols[pred.Col]
	switch pred.Op {
	case "=":
		if col.typ != fromSQLType(pred.Lit.Type) {
			return false, nil // literal type mismatch: fall back to the scan
		}
		enc := string(indexVal(fromSQLValue(pred.Lit)))
		for _, pk := range ix.entries[enc] {
			if !inPKRange([]byte(pk), plLower, plUpper) {
				continue
			}
			if err := fn([]byte(pk)); err != nil {
				return true, err
			}
		}
		return true, nil
	case "LIKE":
		if col.typ != tString || pred.Lit.Type != sqlx.TypeString || len(ix.cols) != 1 {
			return false, nil
		}
		prefix := string(indexStringPrefix(pred.Lit.Str))
		for enc, pks := range ix.entries {
			if !strings.HasPrefix(enc, prefix) {
				continue
			}
			for _, pk := range pks {
				if !inPKRange([]byte(pk), plLower, plUpper) {
					continue
				}
				if err := fn([]byte(pk)); err != nil {
					return true, err
				}
			}
		}
		return true, nil
	}
	return false, nil
}

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"

	sqlx "ydbgo/internal/sql"
)

var (
	_ sqlx.Engine            = (*Engine)(nil)
	_ sqlx.BatchInsertEngine = (*Engine)(nil)
)

// CreateTable implements sqlx.Engine.
func (e *Engine) CreateTable(s *sqlx.TableSchema) error {
	cols := make([]colInfo, len(s.Columns))
	for i, c := range s.Columns {
		cols[i] = colInfo{name: c.Name, typ: fromSQLType(c.Type), notNull: c.NotNull}
	}
	pk := make([]int, 0, len(s.PK))
	for _, p := range s.PK {
		for i, c := range s.Columns {
			if c.Name == p {
				pk = append(pk, i)
			}
		}
	}
	return e.createTableEngine(s.Name, cols, pk, engineOf(s.Engine), s.Retention)
}

// createTableInt is raw storage creation.
func (e *Engine) createTableInt(name string, cols []colInfo, pk []int) error {
	return e.createTableEngine(name, cols, pk, "", 0)
}

func (e *Engine) createTableEngine(name string, cols []colInfo, pk []int, engine string, retention time.Duration) error {
	unlock := e.writeLock()
	defer unlock()
	if _, ok := e.tables[name]; ok {
		return &existsError{name: name}
	}
	if engine != "" && engine != "TABLE" {
		if _, err := e.engineStore(engine); err != nil {
			return err
		}
	}
	t := &table{
		name:      name,
		cols:      cols,
		pk:        pk,
		indexes:   map[string]*index{},
		engine:    engine,
		retention: retention,
	}
	def := e.encodeCreateTable(t)
	err := e.write(func(tx storeTx) error {
		return tx.schemaPut(name, def)
	})
	if err != nil {
		return err
	}
	e.tables[name] = t
	return nil
}

// DropTable removes a schema (from the default store) and a table's rows
// (from the engine's own store).
func (e *Engine) DropTable(name string) error {
	unlock := e.writeLock()
	defer unlock()
	t, ok := e.tables[name]
	if !ok {
		return notFoundError{table: name}
	}
	rowStore := e.store(t.engine)
	if err := e.writeTo(e.store(""), func(tx storeTx) error {
		return tx.schemaDelete(name)
	}); err != nil {
		return err
	}
	if err := e.writeTo(rowStore, func(tx storeTx) error {
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
	return nil
}

// GetSchema implements sqlx.Engine.
func (e *Engine) GetSchema(name string) (*sqlx.TableSchema, error) {
	unlock := e.readLock()
	t, err := e.getTable(name)
	unlock()
	if err != nil {
		return nil, err
	}
	s := &sqlx.TableSchema{Name: t.name, Engine: t.engine, Retention: t.retention}
	for i, c := range t.cols {
		cd := sqlx.ColumnDef{Name: c.name, Type: toSQLType(c.typ), NotNull: c.notNull}
		for _, p := range t.pk {
			if p == i {
				cd.AsPrimary = true
			}
		}
		s.Columns = append(s.Columns, cd)
	}
	for _, p := range t.pk {
		s.PK = append(s.PK, t.cols[p].name)
	}
	return s, nil
}

// Insert implements sqlx.Engine.
func (e *Engine) Insert(table string, row map[string]sqlx.Value) error {
	return e.PutBySQL(table, row)
}

// PutBySQL inserts/overwrites a row from SQL values.
func (e *Engine) PutBySQL(name string, vals map[string]sqlx.Value) error {
	sv := make(map[string]sqlValue, len(vals))
	for k, v := range vals {
		sv[k] = fromSQLValue(v)
	}
	return e.Put(name, sv)
}

// Put writes or overwrites a row (by primary key).
func (e *Engine) Put(name string, vals map[string]sqlValue) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(name)
	if err != nil {
		return err
	}
	return e.writeTo(e.store(t.engine), func(tx storeTx) error {
		return putRow(t, tx, vals)
	})
}

// putRow writes one row inside an open transaction, maintaining index entries.
// An overwrite replaces the previous row: its old index entries are dropped
// before the new value is indexed (idempotent under raft replay).
func putRow(t *table, tx storeTx, vals map[string]sqlValue) error {
	key := schemaKey(t, vals)
	if len(t.indexes) > 0 {
		old, err := tx.rowGet(t.name, []byte(key))
		if err != nil {
			return err
		}
		if len(old) > 0 {
			ov, err := decodeRow(old, t)
			if err != nil {
				return err
			}
			removeRowIndexes(t, ov, key)
		}
	}
	if err := tx.rowPutCells(t.name, []byte(key), encodeRowCells(vals, t)); err != nil {
		return err
	}
	addRowIndexes(t, vals, key)
	return nil
}

// BatchInsert inserts many rows of one table inside a single transaction (one
// write lock, one store commit), sharing the per-row index maintenance. It
// implements sqlx.BatchInsertEngine.
func (e *Engine) BatchInsert(table string, rows []map[string]sqlx.Value) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return 0, err
	}
	var affected int64
	err = e.writeTo(e.store(t.engine), func(tx storeTx) error {
		for _, row := range rows {
			sv := make(map[string]sqlValue, len(row))
			for k, v := range row {
				sv[k] = fromSQLValue(v)
			}
			if err := putRow(t, tx, sv); err != nil {
				return err
			}
			affected++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return affected, nil
}

// Update implements sqlx.Engine.
func (e *Engine) Update(table string, pkValues []sqlx.Value, set map[string]sqlx.Value) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return err
	}
	key := make([]sqlValue, len(pkValues))
	for i, v := range pkValues {
		key[i] = fromSQLValue(v)
	}
	pkKey := encodeKey(key)
	st := e.store(t.engine)
	var old map[string]sqlValue
	e.readFrom(st, func(tx storeTx) error {
		data, err := tx.rowGet(t.name, []byte(pkKey))
		if err != nil {
			return err
		}
		if data == nil {
			return nil
		}
		old, err = decodeRow(data, t)
		return err
	})
	if old == nil {
		return nil
	}
	merged := make(map[string]sqlValue, len(old)+len(set))
	for k, v := range old {
		merged[k] = v
	}
	for k, v := range set {
		merged[k] = fromSQLValue(v)
	}
	newKey := schemaKey(t, merged)
	removeRowIndexes(t, old, pkKey)
	return e.writeTo(st, func(tx storeTx) error {
		if newKey != pkKey {
			if err := tx.rowDelete(t.name, []byte(pkKey)); err != nil {
				return err
			}
		}
		if err := tx.rowPut(t.name, []byte(newKey), encodeRow(merged, t)); err != nil {
			return err
		}
		addRowIndexes(t, merged, newKey)
		return nil
	})
}

// Delete implements sqlx.Engine.
func (e *Engine) Delete(table string, pkValues []sqlx.Value) error {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return err
	}
	key := make([]sqlValue, len(pkValues))
	for i, v := range pkValues {
		key[i] = fromSQLValue(v)
	}
	pkKey := encodeKey(key)
	st := e.store(t.engine)
	return e.writeTo(st, func(tx storeTx) error {
		if len(t.indexes) > 0 {
			old, err := tx.rowGet(t.name, []byte(pkKey))
			if err != nil {
				return err
			}
			if len(old) > 0 {
				ov, err := decodeRow(old, t)
				if err != nil {
					return err
				}
				removeRowIndexes(t, ov, pkKey)
			}
		}
		return tx.rowDelete(t.name, []byte(pkKey))
	})
}

// DeleteRange implements sqlx.Engine: it removes every CSTORE row whose
// encoded PK lies inside r (nil bounds = unbounded), deleting the row marker
// and every column cell of each affected row in one transaction.
func (e *Engine) DeleteRange(table string, r *sqlx.PKRange) (int64, error) {
	var affected int64
	unlock := e.writeLock()
	err := func() error {
		defer unlock()
		t, err := e.getTable(table)
		if err != nil {
			return err
		}
		if t.engine != "CSTORE" && t.engine != "CSTORE2" {
			return fmt.Errorf("range delete requires a columnar table")
		}
		plLower, plUpper := PKRangeBytes(r)
		return e.writeTo(e.store(t.engine), func(tx storeTx) error {
			ct, ok := tx.(columnarTx)
			if !ok {
				return errors.New("range delete requires a columnar store")
			}
			var pks [][]byte
			if err := ct.colRowKeysRange(t.name, plLower, plUpper, func(pk []byte) error {
				pks = append(pks, append([]byte(nil), pk...))
				return nil
			}); err != nil {
				return err
			}
			for _, pk := range pks {
				if len(t.indexes) > 0 {
					old, err := tx.rowGet(t.name, pk)
					if err != nil {
						return err
					}
					if len(old) > 0 {
						ov, err := decodeRow(old, t)
						if err != nil {
							return err
						}
						removeRowIndexes(t, ov, string(pk))
					}
				}
				if err := ct.rowDelete(t.name, pk); err != nil {
					return err
				}
			}
			affected = int64(len(pks))
			return nil
		})
	}()
	if err != nil {
		return 0, err
	}
	// Reclaim space immediately when not inside a raft group-commit batch
	// (the store commit just happened synchronously): drop superseded versions.
	if !e.txActive() {
		if _, cerr := e.Compact(table); cerr != nil {
			return affected, cerr
		}
	}
	return affected, nil
}

// Compact physically reclaims space for a columnar table. For CSTORE it drops
// superseded KV versions keeping only the newest of each key (the companion to
// range deletes); for CSTORE2 it merges the table's mem + parts, dropping
// tombstoned rows and rewriting fresh parts.
func (e *Engine) Compact(table string) (int64, error) {
	unlock := e.writeLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return 0, err
	}
	switch st := e.store(t.engine).(type) {
	case *cstoreStore:
		return st.st.Compact(st.st.Latest())
	case *mpartStore:
		return st.compactTable(t.name)
	}
	return 0, errors.New("compaction requires a columnar store")
}

// Scan implements sqlx.Engine.
func (e *Engine) Scan(name string) ([]sqlx.Row, error) {
	return e.scanRows(name)
}

func (e *Engine) scanRows(name string) ([]sqlx.Row, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(name)
	if err != nil {
		return nil, err
	}
	var rows []sqlx.Row
	e.readFrom(e.store(t.engine), func(tx storeTx) error {
		return tx.rowEach(t.name, func(k, v []byte) error {
			vals, err := decodeRow(v, t)
			if err != nil {
				return err
			}
			row := make(sqlx.Row, len(t.cols))
			for i, c := range t.cols {
				row[i] = toSQLValue(vals[c.name])
			}
			rows = append(rows, row)
			return nil
		})
	})
	return rows, nil
}

// EncodePK produces the sortable encoded primary key string for a row of PK
// values (used by the shard router for range ownership checks).
func EncodePK(values []sqlx.Value) string {
	sv := make([]sqlValue, len(values))
	for i, v := range values {
		sv[i] = fromSQLValue(v)
	}
	return encodeKey(sv)
}

func (e *Engine) getTable(name string) (*table, error) {
	t, ok := e.tables[name]
	if !ok {
		return nil, notFoundError{table: name}
	}
	return t, nil
}

// schemaKey produces the primary key string for a row.
func schemaKey(t *table, vals map[string]sqlValue) string {
	keys := make([]sqlValue, 0, len(t.pk))
	for _, pkIdx := range t.pk {
		name := t.cols[pkIdx].name
		keys = append(keys, vals[name])
	}
	return encodeKey(keys)
}

// encodeRow serializes a row's values in column order.
func encodeRow(vals map[string]sqlValue, t *table) []byte {
	b := makeBuilder()
	b.Var(int64(len(t.cols)))
	for _, c := range t.cols {
		b.Variant(vals[c.name])
	}
	return b.Bytes()
}

// encodeRowCells encodes each column's value into its own cell blob, in table
// column order, without the intermediate full-row blob that encodeRow produces.
// Cells are laid out exactly as splitRow would produce them from the row blob,
// so a store that stores column-major can use them directly.
func encodeRowCells(vals map[string]sqlValue, t *table) [][]byte {
	cells := make([][]byte, len(t.cols))
	for i, c := range t.cols {
		b := makeBuilder()
		b.Variant(vals[c.name])
		cells[i] = b.Bytes()
	}
	return cells
}

func decodeRow(data []byte, t *table) (map[string]sqlValue, error) {
	r := makeReader(data)
	n := int(r.Var())
	vals := make(map[string]sqlValue, len(t.cols))
	for i := 0; i < n && i < len(t.cols); i++ {
		vals[t.cols[i].name] = r.Variant()
	}
	if r.err != nil {
		return nil, r.err
	}
	return vals, nil
}

func encodeKey(vals []sqlValue) string {
	var b bytes.Buffer
	for _, v := range vals {
		b.WriteByte(byte(v.typ))
		b.WriteByte(0xff) // separator guard
		switch v.typ {
		case tInt:
			bb := make([]byte, 8)
			u := uint64(v.i) + 1<<63
			for i := 0; i < 8; i++ {
				bb[i] = byte(u >> (56 - i*8))
			}
			b.Write(bb)
		case tFloat:
			bb := make([]byte, 8)
			u := mathFloatToSortable(v.f)
			for i := 0; i < 8; i++ {
				bb[i] = byte(u >> (56 - i*8))
			}
			b.Write(bb)
		case tString:
			b.Write([]byte(v.s))
			b.WriteByte(0)
		case tBool:
			if v.b {
				b.WriteByte(2)
			} else {
				b.WriteByte(1)
			}
		case tTimestamp:
			bb := make([]byte, 8)
			u := uint64(v.i) + 1<<63
			for i := 0; i < 8; i++ {
				bb[i] = byte(u >> (56 - i*8))
			}
			b.Write(bb)
		}
		b.WriteByte(0xff)
	}
	return string(b.Bytes())
}

func mathFloatToSortable(f float64) uint64 {
	u := math.Float64bits(f)
	if u&(1<<63) != 0 {
		return ^u
	}
	return u | (1 << 63)
}

func toSQLValue(v sqlValue) sqlx.Value {
	typ := toSQLType(v.typ)
	switch typ {
	case sqlx.TypeInt:
		if v.null {
			return sqlx.NullValue
		}
		return sqlx.IntValue(v.i)
	case sqlx.TypeFloat:
		if v.null {
			return sqlx.NullValue
		}
		return sqlx.FloatValue(v.f)
	case sqlx.TypeString:
		if v.null {
			return sqlx.NullValue
		}
		return sqlx.StrValue(v.s)
	case sqlx.TypeBool:
		if v.null {
			return sqlx.NullValue
		}
		return sqlx.BoolValue(v.b)
	case sqlx.TypeTimestamp:
		if v.null {
			return sqlx.NullValue
		}
		return sqlx.TimestampValue(time.UnixMicro(v.i))
	}
	return sqlx.NullValue
}

func fromSQLValue(v sqlx.Value) sqlValue {
	switch v.Type {
	case sqlx.TypeInt:
		if v.Null {
			return sqlValue{typ: tInt, null: true}
		}
		return sqlValue{typ: tInt, i: v.Int}
	case sqlx.TypeFloat:
		if v.Null {
			return sqlValue{typ: tFloat, null: true}
		}
		return sqlValue{typ: tFloat, f: v.Flt}
	case sqlx.TypeString:
		if v.Null {
			return sqlValue{typ: tString, null: true}
		}
		return sqlValue{typ: tString, s: v.Str}
	case sqlx.TypeBool:
		if v.Null {
			return sqlValue{typ: tBool, null: true}
		}
		return sqlValue{typ: tBool, b: v.Bool}
	case sqlx.TypeTimestamp:
		if v.Null {
			return sqlValue{typ: tTimestamp, null: true}
		}
		// Microseconds since epoch: int64 µs covers ~±292k years, so far-future
		// timestamps (e.g. year 9999 bound) don't overflow UnixNano's int64 ns.
		return sqlValue{typ: tTimestamp, i: v.Tm.UnixMicro()}
	}
	return sqlValue{typ: tString, null: true}
}

// existsError signals that a table already exists.
type existsError struct{ name string }

func (e *existsError) Error() string { return "table " + e.name + " already exists" }

// engineOf normalizes a storage engine name.
func engineOf(e string) string {
	switch e {
	case "KV":
		return "KV"
	case "CSTORE", "CSTORE2":
		return e
	default:
		return "TABLE"
	}
}

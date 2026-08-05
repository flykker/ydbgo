package storage

import (
	"bytes"
	"math"
	"time"

	sqlx "ydbgo/internal/sql"
)

var _ sqlx.Engine = (*Engine)(nil)

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
	return e.createTableInt(s.Name, cols, pk)
}

// createTableInt is raw storage creation.
func (e *Engine) createTableInt(name string, cols []colInfo, pk []int) error {
	unlock := e.writeLock()
	defer unlock()
	if _, ok := e.tables[name]; ok {
		return &existsError{name: name}
	}
	t := &table{
		name:    name,
		cols:    cols,
		pk:      pk,
		indexes: map[string]*index{},
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

// DropTable removes a schema and its rows.
func (e *Engine) DropTable(name string) error {
	unlock := e.writeLock()
	defer unlock()
	if _, ok := e.tables[name]; !ok {
		return notFoundError{table: name}
	}
	if err := e.write(func(tx storeTx) error {
		return tx.dropTable(name)
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
	s := &sqlx.TableSchema{Name: t.name}
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
	key := schemaKey(t, vals)
	return e.write(func(tx storeTx) error {
		return tx.rowPut(t.name, []byte(key), encodeRow(vals, t))
	})
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
	var old map[string]sqlValue
	e.read(func(tx storeTx) error {
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
	for k, v := range set {
		old[k] = fromSQLValue(v)
	}
	newKey := schemaKey(t, old)
	return e.write(func(tx storeTx) error {
		if newKey != pkKey {
			if err := tx.rowDelete(t.name, []byte(pkKey)); err != nil {
				return err
			}
		}
		return tx.rowPut(t.name, []byte(newKey), encodeRow(old, t))
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
	return e.write(func(tx storeTx) error {
		return tx.rowDelete(t.name, []byte(encodeKey(key)))
	})
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
	e.read(func(tx storeTx) error {
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
		return sqlx.TimestampValue(time.Unix(0, v.i))
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
		return sqlValue{typ: tTimestamp, i: v.Tm.UnixNano()}
	}
	return sqlValue{typ: tString, null: true}
}

// existsError signals that a table already exists.
type existsError struct{ name string }

func (e *existsError) Error() string { return "table " + e.name + " already exists" }

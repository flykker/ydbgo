package storage

import (
	sqlx "ydbgo/internal/sql"
)

// WAL record types.
const (
	recCreateTable byte = 1
	recDropTable   byte = 2
	recMutate      byte = 3
)

// marshals a create-table record: table metadata + rows (none at creation).
func (e *Engine) encodeCreateTable(t *table) []byte {
	b := newBuilder()
	b.Byte(recCreateTable)
	b.Str(t.name)
	b.Var(int64(len(t.cols)))
	for _, c := range t.cols {
		b.Str(c.name)
		b.Byte(byte(c.typ))
		b.Bool(c.notNull)
	}
	b.Var(int64(len(t.pk)))
	for _, p := range t.pk {
		b.Var(int64(p))
	}
	b.Var(int64(len(t.indexes)))
	for _, ix := range t.indexes {
		// indexes get rebuilt from data; only record definition
		b.Str(ix.name)
		b.Var(int64(len(ix.cols)))
		for _, c := range ix.cols {
			b.Var(int64(c))
		}
	}
	return b.Bytes()
}

func (e *Engine) unmarshalCreateTable(buf []byte) (*table, error) {
	r := makeReader(buf)
	r.Byte() // recCreateTable
	t := &table{indexes: map[string]*index{}}
	t.name = r.Str()
	nc := int(r.Var())
	for i := 0; i < nc; i++ {
		c := colInfo{}
		c.name = r.Str()
		c.typ = sqlType(r.Byte())
		c.notNull = r.Bool()
		t.cols = append(t.cols, c)
	}
	np := int(r.Var())
	for i := 0; i < np; i++ {
		t.pk = append(t.pk, int(r.Var()))
	}
	ni := int(r.Var())
	for i := 0; i < ni; i++ {
		ix := &index{name: r.Str(), entries: map[string][]string{}}
		icc := int(r.Var())
		for j := 0; j < icc; j++ {
			ix.cols = append(ix.cols, int(r.Var()))
		}
		t.indexes[ix.name] = ix
	}
	if r.err != nil {
		return nil, r.err
	}
	return t, nil
}

// marshalDropTable
func (e *Engine) marshalDropTable(name string) []byte {
	b := makeBuilder()
	b.Byte(recDropTable)
	b.Str(name)
	return b.Bytes()
}

func (e *Engine) unmarshalDropTable(buf []byte) (string, error) {
	r := makeReader(buf)
	r.Byte()
	return r.Str(), r.err
}

// Map sql types to storage types.
func fromSQLType(t sqlx.Type) sqlType {
	switch t {
	case sqlx.TypeInt:
		return tInt
	case sqlx.TypeFloat:
		return tFloat
	case sqlx.TypeString:
		return tString
	case sqlx.TypeBool:
		return tBool
	case sqlx.TypeTimestamp:
		return tTimestamp
	}
	return tString
}

func toSQLType(t sqlType) sqlx.Type {
	switch t {
	case tInt:
		return sqlx.TypeInt
	case tFloat:
		return sqlx.TypeFloat
	case tString:
		return sqlx.TypeString
	case tBool:
		return sqlx.TypeBool
	case tTimestamp:
		return sqlx.TypeTimestamp
	}
	return sqlx.TypeNull
}

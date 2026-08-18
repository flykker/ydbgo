package storage

import (
	"encoding/binary"
	"errors"
	"math"
)

type byteReader struct {
	*reader
}

func (r *byteReader) ReadByte() (byte, error) {
	if r.err != nil || r.pos >= len(r.buf) {
		return 0, errors.New("reader: short reader")
	}
	v := r.buf[r.pos]
	r.pos++
	return v, nil
}

type reader struct {
	buf []byte
	pos int
	err error
}

func makeReader(buf []byte) *reader { return &reader{buf: buf} }

func (r *reader) Byte() byte {
	if r.err != nil || r.pos >= len(r.buf) {
		r.err = errors.New("reader: short reader")
		return 0
	}
	v := r.buf[r.pos]
	r.pos++
	return v
}
func (r *reader) Bool() bool { return r.Byte() == 1 }
func (r *reader) Var() int64 {
	if r.err != nil {
		return 0
	}
	u, err := binary.ReadUvarint(&byteReader{r})
	if err != nil {
		r.err = err
		return 0
	}
	// zigzag decode
	return int64(u>>1) ^ -int64(u&1)
}
func (r *reader) Str() string {
	n := int(r.Var())
	if r.err != nil || r.pos+n > len(r.buf) {
		r.err = errors.New("reader: string out of range")
		return ""
	}
	s := string(r.buf[r.pos : r.pos+n])
	r.pos += n
	return s
}
func (r *reader) Variant() sqlValue {
	v := sqlValue{typ: sqlType(r.Byte())}
	v.null = r.Bool()
	switch v.typ {
	case tInt:
		v.i = r.Var()
	case tFloat:
		v.f = math.Float64frombits(uint64(r.Var()))
	case tString:
		v.s = r.Str()
	case tBool:
		v.b = r.Bool()
	case tTimestamp:
		v.i = r.Var()
	}
	return v
}

// builder writes length-prefixed records.
type builder struct {
	buf []byte
}

func makeBuilder() *builder { return &builder{} }
func newBuilder() *builder  { return &builder{} }

func (b *builder) Byte(v byte) {
	b.buf = append(b.buf, v)
}
func (b *builder) Bool(v bool) {
	if v {
		b.buf = append(b.buf, 1)
	} else {
		b.buf = append(b.buf, 0)
	}
}
func (b *builder) Var(v int64) {
	u := uint64(v)<<1 ^ uint64(v>>63) // zigzag
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], u)
	b.buf = append(b.buf, tmp[:n]...)
}
func (b *builder) Str(s string) {
	b.Var(int64(len(s)))
	b.buf = append(b.buf, s...)
}
func (b *builder) Variant(v sqlValue) {
	b.Byte(byte(v.typ))
	b.Bool(v.null)
	switch v.typ {
	case tInt:
		b.Var(v.i)
	case tFloat:
		b.Var(int64(math.Float64bits(v.f)))
	case tString:
		b.Str(v.s)
	case tBool:
		b.Bool(v.b)
	case tTimestamp:
		b.Var(v.i)
	}
}
func (b *builder) Bytes() []byte { return b.buf }

// decodeNumericCell decodes a cell of a known numeric type directly from its
// raw bytes, skipping the generic reader allocation. Cell layout is
// [type][null][zigzag varint]. Returns the value as (intVal, floatVal), whether
// the cell was null, and ok=false if the cell is not shaped as expected.
func decodeNumericCell(cell []byte, want sqlType) (int64, float64, bool, bool) {
	if len(cell) < 2 {
		return 0, 0, false, false
	}
	if want == tFloat && len(cell) >= 2 && sqlType(cell[0]) != tFloat {
		return 0, 0, false, false
	}
	if cell[1] == 1 {
		return 0, 0, true, true // null
	}
	u, n := binary.Uvarint(cell[2:])
	if n <= 0 {
		return 0, 0, false, false
	}
	i := int64(u>>1) ^ -int64(u&1)
	if want == tFloat {
		return 0, math.Float64frombits(uint64(i)), false, true
	}
	return i, 0, false, true
}

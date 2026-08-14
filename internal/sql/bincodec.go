package sql

// Binary statement encoding: raft entries carry a compact, length-prefixed
// sequence of Statement ops instead of SQL text, so followers apply without
// re-parsing (the leader's parse result is replicated verbatim) and the entry
// is smaller. The format:
//
//	uvarint n            number of statements
//	repeated {byte type; fields}
//
// Operators are a fixed small alphabet encoded as byte indexes.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	bStmtCreateTable byte = 1
	bStmtDropTable   byte = 2
	bStmtCreateIndex byte = 3
	bStmtDropIndex   byte = 4
	bStmtInsert      byte = 5
	bStmtUpdate      byte = 6
	bStmtDelete      byte = 7
	bStmtBegin       byte = 8
	bStmtCommit      byte = 9
	bStmtRollback    byte = 10
	bStmtCreateDB    byte = 11
	bStmtSelect      byte = 12
	bStmtKVPut       byte = 13
	bStmtKVGet       byte = 14
	bStmtKVDelete    byte = 15
	bStmtKVScan      byte = 16
)

const (
	bExprLiteral byte = 1
	bExprIdent   byte = 2
	bExprBinary  byte = 3
	bExprUnary   byte = 4
	bExprCall    byte = 5
	bExprCast    byte = 6
)

var binaryOps = []string{"+", "-", "*", "/", "=", "<", ">", "<=", ">=", "<>", "AND", "OR"}
var unaryOps = []string{"NOT", "-", "+"}

func opIndex(list []string, op string) int {
	for i, s := range list {
		if s == op {
			return i
		}
	}
	return -1
}

type bWriter struct{ buf []byte }

func (w *bWriter) uvarint(v uint64) {
	var t [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(t[:], v)
	w.buf = append(w.buf, t[:n]...)
}
func (w *bWriter) varint(v int64)  { w.uvarint(uint64(v)) }
func (w *bWriter) str(s string)    { w.uvarint(uint64(len(s))); w.buf = append(w.buf, s...) }
func (w *bWriter) bool(v bool)     { w.byte(b2b(v)) }
func (w *bWriter) byte(v byte)     { w.buf = append(w.buf, v) }
func (w *bWriter) float(v float64) { w.varint(int64(math.Float64bits(v))) }
func (w *bWriter) typeByte(t Type) { w.byte(byte(t)) }
func (w *bWriter) bytes(b []byte)  { w.uvarint(uint64(len(b))); w.buf = append(w.buf, b...) }

func b2b(v bool) byte {
	if v {
		return 1
	}
	return 0
}

type bReader struct {
	buf []byte
	pos int
	err error
}

func (r *bReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		r.err = errors.New("sql: bad varint")
		return 0
	}
	r.pos += n
	return v
}
func (r *bReader) varint() int64 {
	u := r.uvarint()
	return int64(u)
}
func (r *bReader) byte() byte {
	if r.err != nil || r.pos >= len(r.buf) {
		r.err = errors.New("sql: short read")
		return 0
	}
	v := r.buf[r.pos]
	r.pos++
	return v
}
func (r *bReader) bool() bool     { return r.byte() == 1 }
func (r *bReader) str() string    { n := int(r.uvarint()); return r.take(n) }
func (r *bReader) float() float64 { return math.Float64frombits(uint64(r.varint())) }
func (r *bReader) typeByte() Type { return Type(r.byte()) }
func (r *bReader) take(n int) string {
	if r.err != nil || r.pos+n > len(r.buf) {
		r.err = errors.New("sql: short read")
		return ""
	}
	s := string(r.buf[r.pos : r.pos+n])
	r.pos += n
	return s
}
func (r *bReader) takeBytes() string { return r.take(int(r.uvarint())) }
func (r *bReader) useErr() error     { return r.err }

// EncodeStatements serializes statements to the compact binary form used as a
// raft entry payload. Encodes every statement type the executor can see.
func EncodeStatements(stmts []Statement) []byte {
	w := &bWriter{}
	w.uvarint(uint64(len(stmts)))
	for _, st := range stmts {
		encodeStmt(w, st)
	}
	return w.buf
}

// DecodeStatements parses a binary raft entry payload back into statements.
func DecodeStatements(data []byte) ([]Statement, error) {
	r := &bReader{buf: data}
	n := int(r.uvarint())
	stmts := make([]Statement, 0, n)
	for i := 0; i < n; i++ {
		st, err := decodeStmt(r)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, st)
	}
	if r.useErr() != nil {
		return nil, r.useErr()
	}
	return stmts, nil
}

func encodeStmt(w *bWriter, st Statement) {
	switch s := st.(type) {
	case *CreateTableStmt:
		w.byte(bStmtCreateTable)
		w.str(s.Name)
		w.uvarint(uint64(len(s.Columns)))
		for _, c := range s.Columns {
			w.str(c.Name)
			w.typeByte(c.Type)
			w.bool(c.NotNull)
			w.bool(c.AsPrimary)
			if c.Default != nil {
				w.bool(true)
				encodeExpr(w, c.Default)
			} else {
				w.bool(false)
			}
		}
		w.uvarint(uint64(len(s.PK)))
		for _, p := range s.PK {
			w.str(p)
		}
		w.bool(s.IfNotExists)
		w.str(s.Engine)
		w.varint(int64(s.Retention))
	case *DropTableStmt:
		w.byte(bStmtDropTable)
		w.str(s.Name)
		w.bool(s.IfExists)
	case *CreateIndexStmt:
		w.byte(bStmtCreateIndex)
		w.str(s.Name)
		w.str(s.Table)
		w.uvarint(uint64(len(s.Columns)))
		for _, c := range s.Columns {
			w.str(c)
		}
		w.bool(s.IfNotExists)
	case *DropIndexStmt:
		w.byte(bStmtDropIndex)
		w.str(s.Name)
		w.str(s.Table)
		w.bool(s.IfExists)
	case *InsertStmt:
		w.byte(bStmtInsert)
		w.str(s.Table)
		w.uvarint(uint64(len(s.Columns)))
		for _, c := range s.Columns {
			w.str(c)
		}
		w.uvarint(uint64(len(s.Rows)))
		for _, row := range s.Rows {
			w.uvarint(uint64(len(row)))
			for _, e := range row {
				encodeExpr(w, e)
			}
		}
	case *UpdateStmt:
		w.byte(bStmtUpdate)
		w.str(s.Table)
		w.uvarint(uint64(len(s.Sets)))
		for _, it := range s.Sets {
			w.str(it.Column)
			encodeExpr(w, it.Value)
		}
		if s.Where != nil {
			w.bool(true)
			encodeExpr(w, s.Where)
		} else {
			w.bool(false)
		}
	case *DeleteStmt:
		w.byte(bStmtDelete)
		w.str(s.Table)
		if s.Where != nil {
			w.bool(true)
			encodeExpr(w, s.Where)
		} else {
			w.bool(false)
		}
	case *BeginStmt:
		w.byte(bStmtBegin)
	case *CommitStmt:
		w.byte(bStmtCommit)
	case *RollbackStmt:
		w.byte(bStmtRollback)
	case *CreateDatabaseStmt:
		w.byte(bStmtCreateDB)
		w.str(s.Name)
	case *SelectStmt:
		w.byte(bStmtSelect)
		encodeSelect(w, s)
	case *KVPutStmt:
		w.byte(bStmtKVPut)
		w.str(s.Table)
		w.bytes([]byte(s.Key))
		w.bytes([]byte(s.Value))
	case *KVGetStmt:
		w.byte(bStmtKVGet)
		w.str(s.Table)
		w.bytes([]byte(s.Key))
	case *KVDeleteStmt:
		w.byte(bStmtKVDelete)
		w.str(s.Table)
		w.bytes([]byte(s.Key))
	case *KVScanStmt:
		w.byte(bStmtKVScan)
		w.str(s.Table)
		w.bytes([]byte(s.Start))
		w.bytes([]byte(s.End))
		w.bool(s.HasStart)
		w.bool(s.HasEnd)
	default:
		panic(fmt.Sprintf("sql: cannot encode %T", st))
	}
}

func encodeSelect(w *bWriter, s *SelectStmt) {
	w.bool(s.Distinct)
	w.uvarint(uint64(len(s.Items)))
	for _, it := range s.Items {
		encodeExpr(w, it.Expr)
		w.str(it.Alias)
	}
	w.str(s.From)
	if s.Where != nil {
		w.bool(true)
		encodeExpr(w, s.Where)
	} else {
		w.bool(false)
	}
	w.uvarint(uint64(len(s.GroupBy)))
	for _, e := range s.GroupBy {
		encodeExpr(w, e)
	}
	w.uvarint(uint64(len(s.OrderBy)))
	for _, o := range s.OrderBy {
		encodeExpr(w, o.Expr)
		w.bool(o.Desc)
	}
	w.bool(s.HasLimit)
	w.varint(s.Limit)
}

func encodeExpr(w *bWriter, e Expr) {
	switch x := e.(type) {
	case *Literal:
		w.byte(bExprLiteral)
		w.typeByte(x.Type)
		switch x.Type {
		case TypeInt, TypeTimestamp:
			w.varint(x.Int)
		case TypeFloat:
			w.float(x.Float)
		case TypeString:
			w.str(x.Str)
		case TypeBool:
			w.bool(x.Bool)
		}
	case *Ident:
		w.byte(bExprIdent)
		w.str(x.Name)
	case *BinaryOp:
		w.byte(bExprBinary)
		w.byte(byte(opIndex(binaryOps, x.Op) + 1))
		encodeExpr(w, x.Left)
		encodeExpr(w, x.Right)
	case *UnaryOp:
		w.byte(bExprUnary)
		w.byte(byte(opIndex(unaryOps, x.Op) + 1))
		encodeExpr(w, x.Expr)
	case *Call:
		w.byte(bExprCall)
		w.str(x.Name)
		w.uvarint(uint64(len(x.Args)))
		for _, a := range x.Args {
			encodeExpr(w, a)
		}
	case *CastExpr:
		w.byte(bExprCast)
		encodeExpr(w, x.Expr)
		w.typeByte(x.Type)
	default:
		panic(fmt.Sprintf("sql: cannot encode expr %T", e))
	}
}

func decodeStmt(r *bReader) (Statement, error) {
	t := r.byte()
	switch t {
	case bStmtCreateTable:
		s := &CreateTableStmt{Name: r.str()}
		nc := int(r.uvarint())
		for i := 0; i < nc; i++ {
			c := ColumnDef{Name: r.str(), Type: r.typeByte(), NotNull: r.bool(), AsPrimary: r.bool()}
			if r.bool() {
				c.Default, _ = decodeExpr(r)
			}
			s.Columns = append(s.Columns, c)
		}
		np := int(r.uvarint())
		for i := 0; i < np; i++ {
			s.PK = append(s.PK, r.str())
		}
		s.IfNotExists = r.bool()
		s.Engine = r.str()
		s.Retention = time.Duration(r.varint())
		return s, nil
	case bStmtDropTable:
		return &DropTableStmt{Name: r.str(), IfExists: r.bool()}, nil
	case bStmtCreateIndex:
		s := &CreateIndexStmt{Name: r.str(), Table: r.str(), IfNotExists: r.bool()}
		nc := int(r.uvarint())
		for i := 0; i < nc; i++ {
			s.Columns = append(s.Columns, r.str())
		}
		return s, nil
	case bStmtDropIndex:
		return &DropIndexStmt{Name: r.str(), Table: r.str(), IfExists: r.bool()}, nil
	case bStmtInsert:
		s := &InsertStmt{Table: r.str()}
		nc := int(r.uvarint())
		for i := 0; i < nc; i++ {
			s.Columns = append(s.Columns, r.str())
		}
		nr := int(r.uvarint())
		for i := 0; i < nr; i++ {
			n := int(r.uvarint())
			row := make([]Expr, 0, n)
			for j := 0; j < n; j++ {
				e, err := decodeExpr(r)
				if err != nil {
					return nil, err
				}
				row = append(row, e)
			}
			s.Rows = append(s.Rows, row)
		}
		return s, nil
	case bStmtUpdate:
		s := &UpdateStmt{Table: r.str()}
		ns := int(r.uvarint())
		for i := 0; i < ns; i++ {
			it := &SetItem{Column: r.str()}
			it.Value, _ = decodeExpr(r)
			s.Sets = append(s.Sets, it)
		}
		if r.bool() {
			s.Where, _ = decodeExpr(r)
		}
		return s, nil
	case bStmtDelete:
		s := &DeleteStmt{Table: r.str()}
		if r.bool() {
			s.Where, _ = decodeExpr(r)
		}
		return s, nil
	case bStmtBegin:
		return &BeginStmt{}, nil
	case bStmtCommit:
		return &CommitStmt{}, nil
	case bStmtRollback:
		return &RollbackStmt{}, nil
	case bStmtCreateDB:
		return &CreateDatabaseStmt{Name: r.str()}, nil
	case bStmtSelect:
		return decodeSelect(r)
	case bStmtKVPut:
		return &KVPutStmt{Table: r.str(), Key: r.takeBytes(), Value: r.takeBytes()}, nil
	case bStmtKVGet:
		return &KVGetStmt{Table: r.str(), Key: r.takeBytes()}, nil
	case bStmtKVDelete:
		return &KVDeleteStmt{Table: r.str(), Key: r.takeBytes()}, nil
	case bStmtKVScan:
		return &KVScanStmt{Table: r.str(), Start: r.takeBytes(), End: r.takeBytes(),
			HasStart: r.bool(), HasEnd: r.bool()}, nil
	}
	return nil, fmt.Errorf("sql: unknown statement type %d", t)
}

func decodeSelect(r *bReader) (Statement, error) {
	s := &SelectStmt{Distinct: r.bool()}
	ni := int(r.uvarint())
	for i := 0; i < ni; i++ {
		e, _ := decodeExpr(r)
		s.Items = append(s.Items, &SelectItem{Expr: e, Alias: r.str()})
	}
	s.From = r.str()
	if r.bool() {
		s.Where, _ = decodeExpr(r)
	}
	ng := int(r.uvarint())
	for i := 0; i < ng; i++ {
		e, _ := decodeExpr(r)
		s.GroupBy = append(s.GroupBy, e)
	}
	no := int(r.uvarint())
	for i := 0; i < no; i++ {
		e, _ := decodeExpr(r)
		s.OrderBy = append(s.OrderBy, &OrderItem{Expr: e, Desc: r.bool()})
	}
	s.HasLimit = r.bool()
	s.Limit = r.varint()
	return s, nil
}

func decodeExpr(r *bReader) (Expr, error) {
	t := r.byte()
	switch t {
	case bExprLiteral:
		typ := r.typeByte()
		x := &Literal{Type: typ}
		switch typ {
		case TypeInt, TypeTimestamp:
			x.Int = r.varint()
		case TypeFloat:
			x.Float = r.float()
		case TypeString:
			x.Str = r.str()
		case TypeBool:
			x.Bool = r.bool()
		}
		return x, nil
	case bExprIdent:
		return &Ident{Name: r.str()}, nil
	case bExprBinary:
		op := r.byte()
		left, _ := decodeExpr(r)
		right, _ := decodeExpr(r)
		if int(op)-1 < 0 || int(op)-1 >= len(binaryOps) {
			return nil, errors.New("sql: bad binary op")
		}
		return &BinaryOp{Op: binaryOps[op-1], Left: left, Right: right}, nil
	case bExprUnary:
		op := r.byte()
		e, _ := decodeExpr(r)
		if int(op)-1 < 0 || int(op)-1 >= len(unaryOps) {
			return nil, errors.New("sql: bad unary op")
		}
		return &UnaryOp{Op: unaryOps[op-1], Expr: e}, nil
	case bExprCall:
		x := &Call{Name: r.str()}
		na := int(r.uvarint())
		for i := 0; i < na; i++ {
			e, _ := decodeExpr(r)
			x.Args = append(x.Args, e)
		}
		return x, nil
	case bExprCast:
		e, _ := decodeExpr(r)
		return &CastExpr{Expr: e, Type: r.typeByte()}, nil
	}
	return nil, fmt.Errorf("sql: unknown expr type %d", t)
}

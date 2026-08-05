package sql

type Node interface{}

type Type int

const (
	TypeNull Type = iota
	TypeInt
	TypeFloat
	TypeString
	TypeBool
	TypeTimestamp
)

func (t Type) String() string {
	switch t {
	case TypeInt:
		return "int64"
	case TypeFloat:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	case TypeTimestamp:
		return "timestamp"
	default:
		return "null"
	}
}

// Expr is an expression node.
type Expr interface{}

type Ident struct {
	Name string
}

type Literal struct {
	Type  Type
	Int   int64
	Float float64
	Str   string
	Bool  bool
}

type BinaryOp struct {
	Op    string // + - * / = < > <= >= <> AND OR
	Left  Expr
	Right Expr
}

type UnaryOp struct {
	Op   string // NOT - +
	Expr Expr
}

type Call struct {
	Name string
	Args []Expr
}

type CastExpr struct {
	Expr Expr
	Type Type
}

// ColumnDef defines a column in CREATE TABLE.
type ColumnDef struct {
	Name      string
	Type      Type
	NotNull   bool
	Default   Expr // may be nil
	AsPrimary bool
}

// Statement is a top-level SQL statement.
type Statement interface{}

type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	PK          []string
	IfNotExists bool
}

type DropTableStmt struct {
	Name     string
	IfExists bool
}

type CreateIndexStmt struct {
	Name        string
	Table       string
	Columns     []string
	IfNotExists bool
}

type DropIndexStmt struct {
	Name     string
	Table    string // optional
	IfExists bool
}

type InsertStmt struct {
	Table   string
	Columns []string
	Rows    [][]Expr // multiple rows: INSERT INTO t VALUES (...),(...)
}

type UpdateStmt struct {
	Table string
	Sets  []*SetItem
	Where Expr
}

type SetItem struct {
	Column string
	Value  Expr
}

type DeleteStmt struct {
	Table string
	Where Expr
}

type SelectStmt struct {
	Distinct bool
	Items    []*SelectItem
	From     string
	Where    Expr
	GroupBy  []Expr
	OrderBy  []*OrderItem
	Limit    int64
	HasLimit bool
}

type SelectItem struct {
	Expr  Expr
	Alias string
}

type OrderItem struct {
	Expr Expr
	Desc bool
}

type BeginStmt struct{}
type CommitStmt struct{}
type RollbackStmt struct{}

type CreateDatabaseStmt struct {
	Name string
}

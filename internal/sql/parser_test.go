package sql

import (
	"testing"
)

func parseOne(t *testing.T, src string) Statement {
	t.Helper()
	stmts, err := Parse(src)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", src, err)
	}
	if len(stmts) != 1 {
		t.Fatalf("Parse(%q) returned %d statements, want 1", src, len(stmts))
	}
	return stmts[0]
}

func TestParseCreateTable(t *testing.T) {
	st := parseOne(t, `CREATE TABLE users (id int64 primary key, name string not null, age int64 default 0)`)
	ct, ok := st.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T", st)
	}
	if ct.Name != "users" {
		t.Errorf("name=%q", ct.Name)
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("columns=%d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].Type != TypeInt || !ct.Columns[0].AsPrimary {
		t.Errorf("col0 bad: %+v", ct.Columns[0])
	}
	if ct.Columns[1].Name != "name" || ct.Columns[1].Type != TypeString || !ct.Columns[1].NotNull {
		t.Errorf("col1 bad: %+v", ct.Columns[1])
	}
}

func TestParseInsertMultiRow(t *testing.T) {
	st := parseOne(t, `INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)`)
	ins, ok := st.(*InsertStmt)
	if !ok {
		t.Fatalf("got %T", st)
	}
	if len(ins.Rows) != 2 {
		t.Fatalf("rows=%d", len(ins.Rows))
	}
}

func TestParseSelect(t *testing.T) {
	st := parseOne(t, `SELECT id, name FROM users WHERE age > 25 ORDER BY id DESC LIMIT 10`)
	sel, ok := st.(*SelectStmt)
	if !ok {
		t.Fatalf("got %T", st)
	}
	if sel.From != "users" {
		t.Errorf("from=%q", sel.From)
	}
	if len(sel.Items) != 2 {
		t.Errorf("items=%d", len(sel.Items))
	}
	if sel.OrderBy == nil || len(sel.OrderBy) != 1 || !sel.OrderBy[0].Desc {
		t.Errorf("orderby bad: %+v", sel.OrderBy)
	}
	if !sel.HasLimit || sel.Limit != 10 {
		t.Errorf("limit bad: %d %v", sel.Limit, sel.HasLimit)
	}
}

func TestParseSelectAggregate(t *testing.T) {
	st := parseOne(t, `SELECT dept, COUNT(*) AS c FROM emp GROUP BY dept`)
	sel, ok := st.(*SelectStmt)
	if !ok {
		t.Fatalf("got %T", st)
	}
	if len(sel.GroupBy) != 1 {
		t.Errorf("groupby=%d", len(sel.GroupBy))
	}
	if sel.Items[1].Alias != "c" {
		t.Errorf("alias=%q", sel.Items[1].Alias)
	}
}

func TestEvalArithmetic(t *testing.T) {
	ctx := map[string]Value{"x": IntValue(10), "y": IntValue(3)}
	e, err := Parse(`SELECT x + y * 2`)
	if err != nil {
		t.Fatal(err)
	}
	sel := e[0].(*SelectStmt)
	v, err := Eval(sel.Items[0].Expr, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v.Int != 16 {
		t.Errorf("got %v", v.Int)
	}
}

func TestEvalComparison(t *testing.T) {
	ctx := map[string]Value{"x": IntValue(10), "y": IntValue(3)}
	e, _ := Parse(`SELECT x > y AND y < 5`)
	sel := e[0].(*SelectStmt)
	v, _ := Eval(sel.Items[0].Expr, ctx)
	if !v.Bool {
		t.Errorf("expected true, got %v", v)
	}
}

func TestEvalNull(t *testing.T) {
	ctx := map[string]Value{"x": NullValue, "y": IntValue(5)}
	e, _ := Parse(`SELECT x = y`)
	sel := e[0].(*SelectStmt)
	v, err := Eval(sel.Items[0].Expr, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !v.Null {
		t.Errorf("expected NULL, got %v", v)
	}
}

func TestParseTransactions(t *testing.T) {
	if _, ok := parseOne(t, `BEGIN`).(*BeginStmt); !ok {
		t.Error("BEGIN")
	}
	if _, ok := parseOne(t, `COMMIT`).(*CommitStmt); !ok {
		t.Error("COMMIT")
	}
}

func TestParseMultiple(t *testing.T) {
	stmts, err := Parse(`CREATE TABLE t (a int64); INSERT INTO t VALUES (1); SELECT * FROM t;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 3 {
		t.Fatalf("got %d stmts", len(stmts))
	}
}

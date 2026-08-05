package storage

import (
	"os"
	"path/filepath"
	"testing"

	sqlx "ydbgo/internal/sql"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestEngineCreateInsertScan(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name: "users",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "name", Type: sqlx.TypeString},
			{Name: "age", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	row := map[string]sqlx.Value{"id": sqlx.IntValue(1), "name": sqlx.StrValue("Bob"), "age": sqlx.IntValue(30)}
	if err := e.Insert("users", row); err != nil {
		t.Fatal(err)
	}
	rows, err := e.Scan("users")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0][0].Int != 1 || rows[0][1].Str != "Bob" || rows[0][2].Int != 30 {
		t.Errorf("row=%v", rows[0])
	}
}

func TestEngineUpdateWalk(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt}, {Name: "b", Type: sqlx.TypeString}},
		PK:      []string{"a"},
	}
	e.CreateTable(schema)
	if err := e.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(1), "b": sqlx.StrValue("x")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Update("t", []sqlx.Value{sqlx.IntValue(1)}, map[string]sqlx.Value{"b": sqlx.StrValue("y")}); err != nil {
		t.Fatal(err)
	}
	rows, _ := e.Scan("t")
	if rows[0][1].Str != "y" {
		t.Errorf("b=%q", rows[0][1].Str)
	}
}

func TestEngineDelete(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt}},
		PK:      []string{"a"},
	}
	e.CreateTable(schema)
	e.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(1)})
	if err := e.Delete("t", []sqlx.Value{sqlx.IntValue(1)}); err != nil {
		t.Fatal(err)
	}
	rows, _ := e.Scan("t")
	if len(rows) != 0 {
		t.Errorf("rows after delete=%d", len(rows))
	}
}

func TestEnginePersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt}},
		PK:      []string{"a"},
	}
	e1.CreateTable(schema)
	e1.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(42)})
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	rows, err := e2.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].Int != 42 {
		t.Errorf("replay bad: %v", rows)
	}
}

func TestSQLEndToEnd(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	run := func(s string) {
		stmts, err := sqlx.Parse(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		for _, st := range stmts {
			if _, err := ex.Execute(st); err != nil {
				t.Fatalf("exec %q: %v", s, err)
			}
		}
	}
	run("CREATE TABLE users (id int64 primary key, name string, age int64)")
	run("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")
	r, err := ex.Execute(mustParse(t, "SELECT name FROM users WHERE age >= 30")[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Str != "Bob" {
		t.Errorf("rows=%v", r.Rows)
	}
	// update
	run("UPDATE users SET age = 31 WHERE name = 'Bob'")
	r, _ = ex.Execute(mustParse(t, "SELECT age FROM users WHERE name = 'Bob'")[0])
	if r.Rows[0][0].Int != 31 {
		t.Errorf("age=%v", r.Rows[0][0])
	}
	// aggregate
	r, _ = ex.Execute(mustParse(t, "SELECT COUNT(*) AS c FROM users")[0])
	if r.Rows[0][0].Int != 2 {
		t.Errorf("count=%v", r.Rows[0][0])
	}
	// delete
	run("DELETE FROM users WHERE name = 'Alice'")
	r, _ = ex.Execute(mustParse(t, "SELECT COUNT(*) AS c FROM users")[0])
	if r.Rows[0][0].Int != 1 {
		t.Errorf("count after delete=%v", r.Rows[0][0])
	}
}

func mustParse(t *testing.T, s string) []sqlx.Statement {
	t.Helper()
	stmts, err := sqlx.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

var _ = os.Stat

func TestEngineSnapshotRoundtrip(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt}, {Name: "name", Type: sqlx.TypeString}},
		PK:      []string{"a"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(1), "name": sqlx.StrValue("x")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(2), "name": sqlx.StrValue("y")}); err != nil {
		t.Fatal(err)
	}

	state, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	// restore into a fresh engine (simulating a raft snapshot install)
	dir2 := filepath.Join(t.TempDir(), "db")
	e2, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	rows, err := e2.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0][0].Int != 1 || rows[1][1].Str != "y" {
		t.Errorf("rows=%v", rows)
	}
	// e2 was rebuilt with a second table to prove ReplaceState swaps fully
	if err := e2.CreateTable(&sqlx.TableSchema{Name: "extra", Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt}}, PK: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	if _, err := e2.Scan("extra"); err == nil {
		t.Errorf("expected 'extra' dropped after ReplaceState")
	}
	// WAL rewrite: reopen e2 and confirm rows survive
	e2.Close()
	e3, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer e3.Close()
	rows, err = e3.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Errorf("rows after reopen=%d", len(rows))
	}
}

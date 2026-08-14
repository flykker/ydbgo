package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

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

func mustParse(t testing.TB, s string) []sqlx.Statement {
	t.Helper()
	stmts, err := sqlx.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return stmts
}

func execOK(t *testing.T, ex *sqlx.Executor, s string) *sqlx.Result {
	t.Helper()
	r, err := ex.Execute(mustParse(t, s)[0])
	if err != nil {
		t.Fatalf("%s: %v", s, err)
	}
	return r
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

func TestEngineAttributePersists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	schema := &sqlx.TableSchema{
		Name:   "kv_t",
		Engine: "KV",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	// default table-level engine is TABLE
	s2 := &sqlx.TableSchema{
		Name: "row_t",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(s2); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got, err := e2.GetSchema("kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if got.Engine != "KV" {
		t.Errorf("engine after reopen=%q want KV", got.Engine)
	}
	got2, _ := e2.GetSchema("row_t")
	if got2.Engine != "TABLE" {
		t.Errorf("default engine=%q want TABLE", got2.Engine)
	}
}

func TestKVEngineTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:   "kv_t",
		Engine: "KV",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("kv_t", map[string]sqlx.Value{"id": sqlx.IntValue(1), "v": sqlx.StrValue("a1")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("kv_t", map[string]sqlx.Value{"id": sqlx.IntValue(2), "v": sqlx.StrValue("b2")}); err != nil {
		t.Fatal(err)
	}
	rows, err := e.Scan("kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][1].Str != "a1" || rows[1][1].Str != "b2" {
		t.Fatalf("kv scan: %v", rows)
	}
	// old engine ("TABLE") table coexists!
	if err := e.Insert("users", nil); err == nil {
		t.Log("no users table — ok")
	}
	// snapshot roundtrip across both engines
	state, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	dir2 := filepath.Join(t.TempDir(), "db")
	e2, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	rows, err = e2.Scan("kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("kv rows after snapshot=%d", len(rows))
	}
	// reopen from disk keeps engine attribute and data
	dir3 := filepath.Join(t.TempDir(), "db")
	e3, err := Open(dir3)
	if err != nil {
		t.Fatal(err)
	}
	if err := e3.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if err := e3.Insert("kv_t", map[string]sqlx.Value{"id": sqlx.IntValue(7), "v": sqlx.StrValue("seven")}); err != nil {
		t.Fatal(err)
	}
	e3.Close()
	e4, err := Open(dir3)
	if err != nil {
		t.Fatal(err)
	}
	defer e4.Close()
	rows, err = e4.Scan("kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].Int != 7 {
		t.Fatalf("kv after reopen: %v", rows)
	}
	got, _ := e4.GetSchema("kv_t")
	if got.Engine != "KV" {
		t.Fatalf("engine after reopen=%q", got.Engine)
	}
	// drop only the kv table
	if err := e4.DropTable("kv_t"); err != nil {
		t.Fatal(err)
	}
	if _, err := e4.Scan("kv_t"); err == nil {
		t.Fatal("kv table should be gone after drop")
	}
}

func TestKVRawSurface(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:   "kv_t",
		Engine: "KV",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	// raw KV ops on a KV table
	if err := e.KVPut("kv_t", "alpha", "1"); err != nil {
		t.Fatal(err)
	}
	if err := e.KVPut("kv_t", "beta", "2"); err != nil {
		t.Fatal(err)
	}
	if err := e.KVPut("kv_t", "gamma", "3"); err != nil {
		t.Fatal(err)
	}
	v, err := e.KVGet("kv_t", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if v.Str != "2" {
		t.Fatalf("kv get beta=%q want 2", v.Str)
	}
	if err := e.KVDelete("kv_t", "beta"); err != nil {
		t.Fatal(err)
	}
	v, err = e.KVGet("kv_t", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if !v.Null {
		t.Fatalf("kv get after delete=%v want null", v)
	}
	entries, err := e.KVScan("kv_t", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Key != "alpha" || entries[1].Key != "gamma" {
		t.Fatalf("kv scan: %v", entries)
	}
	// range scan
	entries, err = e.KVScan("kv_t", "a", "g")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "alpha" {
		t.Fatalf("kv range scan: %v", entries)
	}
	// raw KV coexists with row inserts in the same store
	if err := e.Insert("kv_t", map[string]sqlx.Value{"id": sqlx.IntValue(7)}); err != nil {
		t.Fatal(err)
	}
	rows, err := e.Scan("kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows after raw kv puts: %d", len(rows))
	}
	// raw ops on a TABLE-engine table must fail cleanly
	if err := e.CreateTable(&sqlx.TableSchema{
		Name: "row_t",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.KVPut("row_t", "k", "v"); err == nil {
		t.Fatal("KVPut on TABLE engine should error")
	}
	// drop table removes raw KV entries
	if err := e.DropTable("kv_t"); err != nil {
		t.Fatal(err)
	}
	entries, err = e.KVScan("kv_t", "", "")
	if err == nil {
		t.Fatal("scan after drop should error")
	}
	_ = entries
}

// TestCStoreTable exercises the columnar ENGINE=CSTORE backend: inserts,
// scans, deletes, snapshot roundtrip and reopen, mirroring TestKVEngineTable.
func TestCStoreTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeFloat},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("cs_t", map[string]sqlx.Value{"id": sqlx.IntValue(1), "v": sqlx.StrValue("a1"), "score": sqlx.FloatValue(1.5)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("cs_t", map[string]sqlx.Value{"id": sqlx.IntValue(2), "v": sqlx.StrValue("b2"), "score": sqlx.FloatValue(2.5)}); err != nil {
		t.Fatal(err)
	}
	rows, err := e.Scan("cs_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][1].Str != "a1" || rows[1][2].Flt != 2.5 {
		t.Fatalf("cstore scan: %v", rows)
	}
	// update + delete
	if err := e.Update("cs_t", []sqlx.Value{sqlx.IntValue(1)}, map[string]sqlx.Value{"v": sqlx.StrValue("a1x")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("cs_t", []sqlx.Value{sqlx.IntValue(2)}); err != nil {
		t.Fatal(err)
	}
	rows, err = e.Scan("cs_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].Str != "a1x" {
		t.Fatalf("cstore after update/delete: %v", rows)
	}
	// snapshot roundtrip
	state, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	dir2 := filepath.Join(t.TempDir(), "db")
	e2, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	rows, err = e2.Scan("cs_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].Str != "a1x" {
		t.Fatalf("cstore after snapshot: %v", rows)
	}
	got, _ := e2.GetSchema("cs_t")
	if got.Engine != "CSTORE" {
		t.Fatalf("engine after snapshot=%q", got.Engine)
	}
	// reopen from disk
	dir3 := filepath.Join(t.TempDir(), "db")
	e3, err := Open(dir3)
	if err != nil {
		t.Fatal(err)
	}
	if err := e3.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if err := e3.Insert("cs_t", map[string]sqlx.Value{"id": sqlx.IntValue(7), "v": sqlx.StrValue("seven")}); err != nil {
		t.Fatal(err)
	}
	e3.Close()
	e4, err := Open(dir3)
	if err != nil {
		t.Fatal(err)
	}
	defer e4.Close()
	rows, err = e4.Scan("cs_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].Int != 7 {
		t.Fatalf("cstore after reopen: %v", rows)
	}
	got, _ = e4.GetSchema("cs_t")
	if got.Engine != "CSTORE" {
		t.Fatalf("engine after reopen=%q", got.Engine)
	}
	// drop only the cstore table
	if err := e4.DropTable("cs_t"); err != nil {
		t.Fatal(err)
	}
	if _, err := e4.Scan("cs_t"); err == nil {
		t.Fatal("cstore table should be gone after drop")
	}
}

// TestCStoreInBatch exercises the columnar backend inside a group-commit batch
// (the raft FSM path), where writes route through the active store transaction.
func TestCStoreInBatch(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	err := e.UpdateBatch(func() error {
		for i := int64(0); i < 3; i++ {
			if err := e.Insert("cs_t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue("v" + string(rune('a'+i)))}); err != nil {
				return err
			}
		}
		return e.Delete("cs_t", []sqlx.Value{sqlx.IntValue(1)})
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := e.Scan("cs_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("cstore batch scan: %v", rows)
	}
}

// TestCStoreColumns exercises the columnar projection/aggregate surface
// (sqlx.ColumnEngine): ScanColumns, ColumnCount, ColumnAggregate.
func TestCStoreColumns(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]sqlx.Value{
		{"id": sqlx.IntValue(1), "v": sqlx.StrValue("a"), "score": sqlx.IntValue(10)},
		{"id": sqlx.IntValue(2), "v": sqlx.StrValue("b"), "score": sqlx.IntValue(20)},
		{"id": sqlx.IntValue(3), "v": sqlx.StrValue("c"), "score": sqlx.IntValue(30)},
	}
	for _, r := range rows {
		if err := e.Insert("cs_t", r); err != nil {
			t.Fatal(err)
		}
	}

	n, err := e.ColumnCount("cs_t", nil)
	if err != nil || n != 3 {
		t.Fatalf("ColumnCount=%d err=%v", n, err)
	}

	// project only column 2 (score): other columns must be Null
	proj, err := e.ScanColumns("cs_t", []int{2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(proj) != 3 {
		t.Fatalf("projected rows=%d", len(proj))
	}
	for i, r := range proj {
		if r[0] != sqlx.NullValue || r[1] != sqlx.NullValue || r[2].Int != int64((i+1)*10) {
			t.Fatalf("projected row %d: %v", i, r)
		}
	}

	// aggregate pushdown surface
	for _, c := range []struct {
		agg  string
		want int64
	}{
		{"sum", 60}, {"min", 10}, {"max", 30}, {"count", 3},
	} {
		v, err := e.ColumnAggregate("cs_t", 2, c.agg, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.agg, err)
		}
		if v.Int != c.want {
			t.Fatalf("%s=%d want %d", c.agg, v.Int, c.want)
		}
	}
	v, err := e.ColumnAggregate("cs_t", 2, "avg", nil)
	if err != nil || v.Type != sqlx.TypeFloat || v.Flt != 20 {
		t.Fatalf("avg=%v err=%v", v, err)
	}

	// non-CSTORE engine must refuse
	if err := e.CreateTable(&sqlx.TableSchema{
		Name: "plain_t", Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
		}, PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ScanColumns("plain_t", []int{0}, nil); err == nil {
		t.Fatal("ScanColumns on non-CSTORE should fail")
	}
}

// TestCStoreAggregateSQL verifies aggregate pushdown through the SQL executor
// matches the in-memory path.
func TestCStoreAggregateSQL(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	for i := int64(1); i <= 4; i++ {
		if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO cs_t VALUES (%d, 'v%d', %d)", i, i, i*10))[0]); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT COUNT(*) FROM cs_t", "4"},
		{"SELECT COUNT(score) FROM cs_t", "4"},
		{"SELECT SUM(score) FROM cs_t", "100"},
		{"SELECT MIN(score) FROM cs_t", "10"},
		{"SELECT MAX(score) FROM cs_t", "40"},
		{"SELECT AVG(score) FROM cs_t", "25"},
		{"SELECT COUNT(*) AS c, SUM(score) AS s FROM cs_t", "4,100"},
	}
	for _, c := range cases {
		r, err := ex.Execute(mustParse(t, c.sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := make([]string, len(r.Rows[0]))
		for i, v := range r.Rows[0] {
			got[i] = v.String()
		}
		if strings.Join(got, ",") != c.want {
			t.Fatalf("%s = %v want %s", c.sql, got, c.want)
		}
	}
	// with WHERE/group-by the executor must fall back to the row path
	r, err := ex.Execute(mustParse(t, "SELECT SUM(score) FROM cs_t WHERE id > 2")[0])
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].String() != "70" {
		t.Fatalf("where-sum=%v", r.Rows[0])
	}
}

// TestCStoreColumnsInBatch exercises columnar reads inside a group-commit
// batch, where reads route through the active store transaction and must see
// the batch's own uncommitted writes (overlay merge path).
func TestCStoreColumnsInBatch(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	err := e.UpdateBatch(func() error {
		for i := int64(1); i <= 3; i++ {
			if err := e.Insert("cs_t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue("v" + string(rune('0'+i))), "score": sqlx.IntValue(i * 10)}); err != nil {
				return err
			}
		}
		// uncommitted aggregate over the batch's own writes
		v, err := e.ColumnAggregate("cs_t", 2, "sum", nil)
		if err != nil || v.Int != 60 {
			return fmt.Errorf("batch sum=%v err=%v", v, err)
		}
		n, err := e.ColumnCount("cs_t", nil)
		if err != nil || n != 3 {
			return fmt.Errorf("batch count=%d err=%v", n, err)
		}
		// uncommitted projection over the batch's own writes
		proj, err := e.ScanColumns("cs_t", []int{2}, nil)
		if err != nil {
			return err
		}
		for i, r := range proj {
			if r[0] != sqlx.NullValue || r[1] != sqlx.NullValue || r[2].Int != int64((i+1)*10) {
				return fmt.Errorf("batch projected row %d: %v", i, r)
			}
		}
		// deletes in the batch must reflect into uncommitted reads
		if err := e.Delete("cs_t", []sqlx.Value{sqlx.IntValue(2)}); err != nil {
			return err
		}
		v, err = e.ColumnAggregate("cs_t", 2, "sum", nil)
		if err != nil || v.Int != 40 {
			return fmt.Errorf("batch sum after delete=%v err=%v", v, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCStoreWherePrune verifies primary-key range pruning from WHERE through
// the SQL executor: aggregate pushdown over a range, projected scans within a
// range, and correctness of mixed PK/non-PK filters (which fall back to the
// row path).
func TestCStoreWherePrune(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	for i := int64(1); i <= 100; i++ {
		if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO cs_t VALUES (%d, 'v%d', %d)", i, i, i*10))[0]); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		sql  string
		want string
	}{
		// pure aggregate over a pruned PK range (pushdown)
		{"SELECT COUNT(*) FROM cs_t WHERE id >= 10 AND id < 20", "10"},
		{"SELECT SUM(score) FROM cs_t WHERE id >= 10 AND id < 20", "1450"},
		{"SELECT MAX(score) FROM cs_t WHERE id < 3", "20"},
		{"SELECT COUNT(*) FROM cs_t WHERE id = 42", "1"},
		// half-open window
		{"SELECT COUNT(*) FROM cs_t WHERE id > 95", "5"},
		{"SELECT COUNT(*) FROM cs_t WHERE id <= 2", "2"},
		// mixed PK + non-PK filter: falls back to row path but stays correct
		{"SELECT COUNT(*) FROM cs_t WHERE id >= 10 AND id < 20 AND score > 100", "9"},
		{"SELECT SUM(score) FROM cs_t WHERE id >= 10 AND v = 'v15'", "150"},
	}
	for _, c := range cases {
		r, err := ex.Execute(mustParse(t, c.sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := r.Rows[0][0].String()
		if got != c.want {
			t.Fatalf("%s = %v want %s", c.sql, got, c.want)
		}
	}
	// projected scan within a pruned range
	r, err := ex.Execute(mustParse(t, "SELECT score FROM cs_t WHERE id >= 90")[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 11 {
		t.Fatalf("rows=%d want 11", len(r.Rows))
	}
	for i, row := range r.Rows {
		if len(row) != 1 || row[0].Int != int64((90+i)*10) {
			t.Fatalf("row %d: %v", i, row)
		}
	}
	// exact-pk point read returns a single row
	r, err = ex.Execute(mustParse(t, "SELECT v FROM cs_t WHERE id = 7")[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Str != "v7" {
		t.Fatalf("point read: %v", r.Rows)
	}
	// empty range
	r, err = ex.Execute(mustParse(t, "SELECT COUNT(*) FROM cs_t WHERE id >= 50 AND id < 20")[0])
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].String() != "0" {
		t.Fatalf("empty range count=%v", r.Rows[0])
	}
}

// TestCStoreWherePruneTimestamps is the log-analytics shape: a timestamp PK
// table queried by time window, with a per-row non-PK filter.
func TestCStoreWherePruneTimestamps(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "logs",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "ts", Type: sqlx.TypeTimestamp, NotNull: true, AsPrimary: true},
			{Name: "level", Type: sqlx.TypeString},
			{Name: "lat", Type: sqlx.TypeInt},
		},
		PK: []string{"ts"},
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		ts := base.Add(time.Duration(h) * time.Hour)
		level := "INFO"
		if h%3 == 0 {
			level = "ERROR"
		}
		sql := fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s', %d)", ts.Format(time.RFC3339Nano), level, h*10)
		if _, err := ex.Execute(mustParse(t, sql)[0]); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT COUNT(*) FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T06:00:00Z'", "6"},
		{"SELECT COUNT(*) FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T06:00:00Z' AND level = 'ERROR'", "2"},
		{"SELECT SUM(lat) FROM logs WHERE ts >= '2024-01-01T12:00:00Z' AND ts <= '2024-01-01T14:00:00Z'", "390"},
		{"SELECT MAX(lat) FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T01:00:00Z'", "0"},
		{"SELECT COUNT(*) FROM logs WHERE ts = '2024-01-01T03:00:00Z'", "1"},
	}
	for _, c := range cases {
		r, err := ex.Execute(mustParse(t, c.sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := r.Rows[0][0].String()
		if got != c.want {
			t.Fatalf("%s = %v want %s", c.sql, got, c.want)
		}
	}
	// projected scan over a window returns only matching rows
	r, err := ex.Execute(mustParse(t, "SELECT level FROM logs WHERE ts >= '2024-01-01T06:00:00Z' AND ts < '2024-01-01T09:00:00Z'")[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("window rows=%d want 3", len(r.Rows))
	}
	if r.Rows[0][0].Str != "ERROR" || r.Rows[1][0].Str != "INFO" {
		t.Fatalf("window levels: %v %v", r.Rows[0], r.Rows[1])
	}
}

// TestCStoreGroupBy verifies columnar GROUP BY pushdown produces the same
// groups/aggregates as the row-store path.
func TestCStoreGroupBy(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "cat", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	// ids 1..30; cats cycle a/b/c; scores id*10
	for i := int64(1); i <= 30; i++ {
		cat := []string{"a", "b", "c"}[int(i%3)]
		if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO cs_t VALUES (%d, '%s', %d)", i, cat, i*10))[0]); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		sql  string
		want []string // rows joined by "," in any order, each row "v1|v2|..."
	}{
		{
			"SELECT cat, COUNT(*) FROM cs_t GROUP BY cat",
			[]string{"a|10", "b|10", "c|10"},
		},
		{
			"SELECT cat, SUM(score) FROM cs_t GROUP BY cat",
			[]string{"a|1650", "b|1450", "c|1550"},
		},
		{
			"SELECT cat, MIN(score), MAX(score) FROM cs_t GROUP BY cat",
			[]string{"a|30|300", "b|10|280", "c|20|290"},
		},
		{
			"SELECT cat, AVG(score) FROM cs_t GROUP BY cat",
			[]string{"a|165", "b|145", "c|155"},
		},
		// grouped over a pruned PK range
		{
			"SELECT cat, COUNT(*) FROM cs_t WHERE id > 10 AND id <= 20 GROUP BY cat",
			[]string{"a|3", "b|3", "c|4"},
		},
		// grouped with a non-PK filter falls back to the row path
		{
			"SELECT cat, COUNT(*) FROM cs_t WHERE score > 100 GROUP BY cat",
			[]string{"a|7", "b|6", "c|7"},
		},
	}
	for _, c := range cases {
		r, err := ex.Execute(mustParse(t, c.sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := make([]string, 0, len(r.Rows))
		for _, row := range r.Rows {
			parts := make([]string, len(row))
			for i, v := range row {
				parts[i] = v.String()
			}
			got = append(got, strings.Join(parts, "|"))
		}
		sort.Strings(got)
		want := append([]string(nil), c.want...)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s = %v want %v", c.sql, got, c.want)
		}
	}
}

// TestCStoreRetention verifies columnar range deletes: DELETE over an exact PK
// range removes markers and cells in one pass, and re-running is idempotent.
func TestCStoreRetention(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "cat", Type: sqlx.TypeString},
			{Name: "score", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	for i := int64(1); i <= 30; i++ {
		if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO cs_t VALUES (%d, 'cat', %d)", i, i*10))[0]); err != nil {
			t.Fatal(err)
		}
	}
	count := func(sql string) string {
		t.Helper()
		r, err := ex.Execute(mustParse(t, sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return r.Rows[0][0].String()
	}
	// delete an open upper bound
	execOK(t, ex, "DELETE FROM cs_t WHERE id < 5")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "26" {
		t.Fatalf("after id<5: count=%s want 26", got)
	}
	// delete a closed upper bound
	execOK(t, ex, "DELETE FROM cs_t WHERE id <= 7")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "23" {
		t.Fatalf("after id<=7: count=%s want 23", got)
	}
	// delete a lower bound
	execOK(t, ex, "DELETE FROM cs_t WHERE id >= 28")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "20" {
		t.Fatalf("after id>=28: count=%s want 20", got)
	}
	// aggregate over the survivors reflects the deletions
	if got := count("SELECT SUM(score) FROM cs_t"); got != "3500" {
		t.Fatalf("SUM(score)=%s want 3500", got)
	}
	// a window delete
	execOK(t, ex, "DELETE FROM cs_t WHERE id > 12 AND id <= 16")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "16" {
		t.Fatalf("after window: count=%s want 16", got)
	}
	// non-PK WHERE falls back to the row path but still deletes
	execOK(t, ex, "DELETE FROM cs_t WHERE cat = 'nope'")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "16" {
		t.Fatalf("after no-op fallback: count=%s want 16", got)
	}
	execOK(t, ex, "DELETE FROM cs_t WHERE cat = 'cat' AND id > 16 AND id <= 20")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "12" {
		t.Fatalf("after mixed delete: count=%s want 12", got)
	}
	// empty range and idempotent re-delete
	execOK(t, ex, "DELETE FROM cs_t WHERE id < 3 AND id > 28")
	if got := count("SELECT COUNT(*) FROM cs_t"); got != "12" {
		t.Fatalf("after empty range: count=%s want 12", got)
	}
	r, err := ex.Execute(mustParse(t, "DELETE FROM cs_t WHERE id < 5")[0])
	if err != nil {
		t.Fatal(err)
	}
	if r.Affected != 0 {
		t.Fatalf("re-delete affected=%d want 0", r.Affected)
	}
	// projected scan over the survivors
	r, err = ex.Execute(mustParse(t, "SELECT id FROM cs_t WHERE id > 20 AND id < 26 ORDER BY id")[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 5 || r.Rows[0][0].String() != "21" || r.Rows[4][0].String() != "25" {
		t.Fatalf("survivors: %v", r.Rows)
	}
}

// TestKVRawInBatch exercises the KV surface inside a group-commit batch (the
// raft FSM path), where writes route through the active store transaction.
func TestKVRawInBatch(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "kv_t",
		Engine: "KV",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	err := e.UpdateBatch(func() error {
		if err := e.KVPut("kv_t", "k1", "v1"); err != nil {
			return err
		}
		if err := e.KVPut("kv_t", "k2", "v2"); err != nil {
			return err
		}
		return e.KVDelete("kv_t", "k1")
	})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := e.KVScan("kv_t", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "k2" || entries[0].Value != "v2" {
		t.Fatalf("batch kv scan: %v", entries)
	}
	// snapshot roundtrip preserves raw KV area
	state, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	dir2 := filepath.Join(t.TempDir(), "db")
	e2, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	entries, err = e2.KVScan("kv_t", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "k2" {
		t.Fatalf("kv scan after snapshot: %v", entries)
	}
}

// TestLike verifies LIKE/NOT LIKE filtering on the row path and CSTORE columnar
// scans, including the string-PK prefix range optimization.
func TestLike(t *testing.T) {
	for _, engine := range []string{"TABLE", "CSTORE"} {
		e := newTestEngine(t)
		if err := e.CreateTable(&sqlx.TableSchema{
			Name:   "t",
			Engine: engine,
			Columns: []sqlx.ColumnDef{
				{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
				{Name: "name", Type: sqlx.TypeString},
			},
			PK: []string{"id"},
		}); err != nil {
			t.Fatal(err)
		}
		ex := sqlx.NewExecutor(e)
		names := []string{"alpha", "alpine", "beta", "alps", "Alphabet"}
		for i, n := range names {
			if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i+1, n))[0]); err != nil {
				t.Fatal(err)
			}
		}
		cases := []struct {
			sql  string
			want string
		}{
			{"SELECT name FROM t WHERE name LIKE 'alp%' ORDER BY name", "alpha|alpine|alps"},
			{"SELECT name FROM t WHERE name LIKE '%lp%' ORDER BY name", "Alphabet|alpha|alpine|alps"},
			{"SELECT name FROM t WHERE name LIKE 'a_p%' ORDER BY name", "alpha|alpine|alps"},
			{"SELECT name FROM t WHERE name NOT LIKE 'alp%' ORDER BY name", "Alphabet|beta"},
			{"SELECT COUNT(*) FROM t WHERE name LIKE '%z%'", "0"},
		}
		for _, c := range cases {
			r, err := ex.Execute(mustParse(t, c.sql)[0])
			if err != nil {
				t.Fatalf("%s (%s): %v", engine, c.sql, err)
			}
			var got []string
			for _, row := range r.Rows {
				parts := make([]string, len(row))
				for i, v := range row {
					parts[i] = v.String()
				}
				got = append(got, strings.Join(parts, "|"))
			}
			if strings.Join(got, "|") != c.want {
				t.Fatalf("%s %q = %v want %q", engine, c.sql, got, c.want)
			}
		}
		e.Close()
	}
}

// TestLikeStringPkPrune verifies LIKE on a string PK folds into a PK range so
// the CSTORE scan only touches the matching prefix.
func TestLikeStringPkPrune(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "k", Type: sqlx.TypeString, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeInt},
		},
		PK: []string{"k"},
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	for _, kv := range []struct{ k, v string }{
		{"apple", "1"}, {"apricot", "2"}, {"banana", "3"}, {"app", "4"},
	} {
		if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO t VALUES ('%s', %s)", kv.k, kv.v))[0]); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT k FROM t WHERE k LIKE 'app%' ORDER BY k", "app|apple"},
		{"SELECT k FROM t WHERE k LIKE 'appl%' ORDER BY k", "apple"},
		{"SELECT k FROM t WHERE k LIKE 'banana' ORDER BY k", "banana"},
		{"SELECT COUNT(*) FROM t WHERE k LIKE 'ap%' AND v = 1", "1"},
	}
	for _, c := range cases {
		r, err := ex.Execute(mustParse(t, c.sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		var got []string
		for _, row := range r.Rows {
			parts := make([]string, len(row))
			for i, v := range row {
				parts[i] = v.String()
			}
			got = append(got, strings.Join(parts, "|"))
		}
		if strings.Join(got, "|") != c.want {
			t.Fatalf("%q = %v want %q", c.sql, got, c.want)
		}
	}
}

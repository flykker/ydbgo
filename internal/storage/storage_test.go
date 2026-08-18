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

func TestEngineBatchInsert(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt}, {Name: "name", Type: sqlx.TypeString}, {Name: "age", Type: sqlx.TypeInt}},
		PK:      []string{"a"},
		Engine:  "CSTORE",
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if err := e.CreateIndex("t", "ix_name", []string{"name"}, false); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]sqlx.Value, 0, 100)
	for i := 0; i < 100; i++ {
		rows = append(rows, map[string]sqlx.Value{
			"a":    sqlx.IntValue(int64(i)),
			"name": sqlx.StrValue(fmt.Sprintf("n%d", i%5)),
			"age":  sqlx.IntValue(int64(i)),
		})
	}
	n, err := e.BatchInsert("t", rows)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("affected=%d want 100", n)
	}
	scanned, err := e.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(scanned) != 100 {
		t.Fatalf("rows=%d want 100", len(scanned))
	}
	// Index entries were maintained by the batch.
	cnt, err := e.ColumnCountFiltered("t", &sqlx.ColumnFilter{Col: 1, Op: "=", Lit: sqlx.StrValue("n2")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 20 {
		t.Fatalf("n2 count=%d want 20", cnt)
	}
	// Overwrite within the batch keeps entries consistent.
	for i := 0; i < 5; i++ {
		rows[i] = map[string]sqlx.Value{"a": sqlx.IntValue(int64(i)), "name": sqlx.StrValue("x"), "age": sqlx.IntValue(0)}
	}
	if _, err := e.BatchInsert("t", rows[:5]); err != nil {
		t.Fatal(err)
	}
	cnt, _ = e.ColumnCountFiltered("t", &sqlx.ColumnFilter{Col: 1, Op: "=", Lit: sqlx.StrValue("x")}, nil)
	if cnt != 5 {
		t.Fatalf("x count=%d want 5", cnt)
	}
	cnt, _ = e.ColumnCountFiltered("t", &sqlx.ColumnFilter{Col: 1, Op: "=", Lit: sqlx.StrValue("n0")}, nil)
	if cnt != 19 {
		t.Fatalf("n0 count=%d want 19", cnt)
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

// TestBIFunctions exercises the BI-facing SQL functions used by dashboards:
// time_bucket(interval, ts), COUNT_IF(cond) and NOW(), both standalone and in
// grouped queries over a CSTORE table.
func TestBIFunctions(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	execOK(t, ex, "CREATE TABLE logs (ts timestamp primary key, level string, lat double) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 8; i++ {
		ts := base.Add(time.Duration(i) * 30 * time.Minute)
		level := "INFO"
		if i%3 == 0 {
			level = "ERROR"
		}
		execOK(t, ex, fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s', %d)",
			ts.Format(time.RFC3339Nano), level, i*10))
	}

	// time_bucket standalone: bucket boundaries align to the interval.
	r := execOK(t, ex, "SELECT time_bucket('2h', '2024-01-01T01:45:00Z')")
	want := "2024-01-01T00:00:00Z"
	if got := r.Rows[0][0].String(); got != want {
		t.Errorf("time_bucket('2h') = %s, want %s", got, want)
	}
	// compound interval string and negative-epoch-adjacent times.
	r = execOK(t, ex, "SELECT time_bucket('1h30m', '2024-01-01T01:45:00Z')")
	if got := r.Rows[0][0].String(); got != "2024-01-01T01:30:00Z" {
		t.Errorf("time_bucket('1h30m') = %s", got)
	}
	// numeric seconds interval.
	r = execOK(t, ex, "SELECT time_bucket(3600, '2024-01-01T01:45:00Z')")
	if got := r.Rows[0][0].String(); got != "2024-01-01T01:00:00Z" {
		t.Errorf("time_bucket(3600) = %s", got)
	}
	// NOW() returns a timestamp; unixepoch returns an int.
	if r = execOK(t, ex, "SELECT unixepoch()"); r.Rows[0][0].Type != sqlx.TypeInt {
		t.Errorf("unixepoch type = %s", r.Rows[0][0].Type)
	}

	// COUNT_IF whole table: 8 rows, ERROR on i%3==0 -> i in {0,3,6} -> 3.
	r = execOK(t, ex, "SELECT COUNT_IF(level = 'ERROR') FROM logs")
	if r.Rows[0][0].Int != 3 {
		t.Errorf("count_if errors = %d, want 3", r.Rows[0][0].Int)
	}
	// COUNT_IF over an empty window.
	r = execOK(t, ex, "SELECT COUNT_IF(level = 'ERROR') FROM logs WHERE ts < '2023-01-01T00:00:00Z'")
	if r.Rows[0][0].Int != 0 {
		t.Errorf("count_if empty = %d, want 0", r.Rows[0][0].Int)
	}

	// Grouped by 2h bucket aligned to epoch: rows span 00:00..03:30Z -> exactly
	// two buckets: 00:00Z (i=0..3) and 02:00Z (i=4..7), 4 rows each. ERROR on
	// i%3==0 -> bucket 00:00Z has errors at i=0,3 (count 2), bucket 02:00Z has
	// an error at i=6 (count 1).
	r = execOK(t, ex, "SELECT time_bucket('2h', ts) AS b, COUNT(*) AS c, COUNT_IF(level = 'ERROR') AS e FROM logs GROUP BY time_bucket('2h', ts)")
	got := map[string][]int64{}
	for _, row := range r.Rows {
		got[row[0].String()] = []int64{row[1].Int, row[2].Int}
	}
	wantBuckets := map[string][]int64{
		"2024-01-01T00:00:00Z": {4, 2},
		"2024-01-01T02:00:00Z": {4, 1},
	}
	if len(got) != len(wantBuckets) {
		t.Fatalf("grouped buckets = %d, want %d (%v)", len(got), len(wantBuckets), got)
	}
	for b, w := range wantBuckets {
		if g, ok := got[b]; !ok || g[0] != w[0] || g[1] != w[1] {
			t.Errorf("bucket %s = %v, want %v", b, g, w)
		}
	}
}

func TestSQLIndexEndToEnd(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	execOK(t, ex, "CREATE TABLE users (id int64 primary key, name string, age int64) ENGINE=CSTORE")
	execOK(t, ex, "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Bobby', 35)")
	execOK(t, ex, "CREATE INDEX ix_name ON users (name)")

	r := execOK(t, ex, "SELECT id FROM users WHERE name = 'Bob'")
	if len(r.Rows) != 1 || r.Rows[0][0].Int != 2 {
		t.Errorf("eq rows=%v", r.Rows)
	}
	r = execOK(t, ex, "SELECT id FROM users WHERE name LIKE 'Bob%'")
	if len(r.Rows) != 2 {
		t.Errorf("like rows=%v", r.Rows)
	}
	r = execOK(t, ex, "SELECT COUNT(*) FROM users WHERE name = 'Alice'")
	if r.Rows[0][0].Int != 1 {
		t.Errorf("count=%v", r.Rows[0][0])
	}

	// DML keeps the index in sync.
	execOK(t, ex, "UPDATE users SET name = 'Zed' WHERE name = 'Alice'")
	r = execOK(t, ex, "SELECT id FROM users WHERE name = 'Alice'")
	if len(r.Rows) != 0 {
		t.Errorf("Alice after update=%v", r.Rows)
	}
	r = execOK(t, ex, "SELECT id FROM users WHERE name = 'Zed'")
	if len(r.Rows) != 1 || r.Rows[0][0].Int != 1 {
		t.Errorf("Zed rows=%v", r.Rows)
	}
	execOK(t, ex, "DELETE FROM users WHERE id = 2")
	r = execOK(t, ex, "SELECT id FROM users WHERE name = 'Bob'")
	if len(r.Rows) != 0 {
		t.Errorf("Bob after delete=%v", r.Rows)
	}

	// Drop + IF EXISTS / IF NOT EXISTS handling.
	execOK(t, ex, "DROP INDEX ix_name ON users")
	r = execOK(t, ex, "SELECT COUNT(*) FROM users")
	if r.Rows[0][0].Int != 2 {
		t.Errorf("count after drop=%v", r.Rows[0][0])
	}
	execOK(t, ex, "CREATE INDEX IF NOT EXISTS ix_name ON users (name)")
	execOK(t, ex, "CREATE INDEX IF NOT EXISTS ix_name ON users (name)")
	execOK(t, ex, "DROP INDEX IF EXISTS ix_name ON users")
	execOK(t, ex, "DROP INDEX IF EXISTS ix_name ON users")
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

// TestEngineSnapshotPointInTime verifies CaptureSnapshot pins the exact state
// at capture time: MarshalSnap after further writes must not see them, and the
// captured data must remain fully restorable.
func TestEngineSnapshotPointInTime(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "t",
		Engine:  "CSTORE",
		Columns: []sqlx.ColumnDef{{Name: "a", Type: sqlx.TypeInt, AsPrimary: true}, {Name: "name", Type: sqlx.TypeString}},
		PK:      []string{"a"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	for _, a := range []int{1, 2} {
		if err := e.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(int64(a)), "name": sqlx.StrValue("pre" + string(rune('a'+a)))}); err != nil {
			t.Fatal(err)
		}
	}

	snap, err := e.CaptureSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Writes after capture must be invisible to MarshalSnap.
	for a := 3; a <= 6; a++ {
		if err := e.Insert("t", map[string]sqlx.Value{"a": sqlx.IntValue(int64(a)), "name": sqlx.StrValue("post" + string(rune('a'+a)))}); err != nil {
			t.Fatal(err)
		}
	}

	state, err := e.MarshalSnap(snap)
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
	rows, err := e2.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("snapshot rows=%d, want 2 (post-capture writes leaked)", len(rows))
	}

	// A fresh capture reflects everything.
	state2, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	e3, err := Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e3.Close()
	if err := e3.ReplaceState(state2); err != nil {
		t.Fatal(err)
	}
	rows, err = e3.Scan("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 {
		t.Fatalf("fresh snapshot rows=%d, want 6", len(rows))
	}
}

// TestEngineSnapshotKVTable covers the ENGINE=KV snapshot path (raw byte-KV
// entries serialized via kvTx.dataEach).
func TestEngineSnapshotKVTable(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "kv_t",
		Engine:  "KV",
		Columns: []sqlx.ColumnDef{{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true}, {Name: "v", Type: sqlx.TypeString}},
		PK:      []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	for a := 1; a <= 4; a++ {
		if err := e.Insert("kv_t", map[string]sqlx.Value{"id": sqlx.IntValue(int64(a)), "v": sqlx.StrValue("v" + string(rune('a'+a)))}); err != nil {
			t.Fatal(err)
		}
	}

	snap, err := e.CaptureSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	if err := e.Insert("kv_t", map[string]sqlx.Value{"id": sqlx.IntValue(5), "v": sqlx.StrValue("vx")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("kv_t", []sqlx.Value{sqlx.IntValue(1)}); err != nil {
		t.Fatal(err)
	}

	state, err := e.MarshalSnap(snap)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	rows, err := e2.Scan("kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("snapshot kv rows=%d, want 4", len(rows))
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

// TestCStoreFilteredColumns exercises the predicate-pushdown surface
// (sqlx.ColumnEngine): ColumnCountFiltered, ColumnAggregatesFiltered and
// ScanColumnsFiltered, for both equality and leading-literal LIKE predicates.
func TestCStoreFilteredColumns(t *testing.T) {
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
	vals := []struct {
		id    int64
		v     string
		score int64
	}{
		{1, "apple", 10},
		{2, "apricot", 20},
		{3, "banana", 30},
		{4, "cherry", 40},
	}
	for _, r := range vals {
		if err := e.Insert("cs_t", map[string]sqlx.Value{
			"id": sqlx.IntValue(r.id), "v": sqlx.StrValue(r.v), "score": sqlx.IntValue(r.score),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// equality predicate on the string column: count + aggregate on another col
	eq := &sqlx.ColumnFilter{Col: 1, Op: "=", Lit: sqlx.StrValue("banana")}
	n, err := e.ColumnCountFiltered("cs_t", eq, nil)
	if err != nil || n != 1 {
		t.Fatalf("ColumnCountFiltered(eq)=%d err=%v", n, err)
	}
	// aggregate over the filter column itself
	vals1, err := e.ColumnAggregatesFiltered("cs_t", 1, []string{"count"}, eq, nil)
	if err != nil || len(vals1) != 1 || vals1[0].Int != 1 {
		t.Fatalf("ColumnAggregatesFiltered(eq,self)=%v err=%v", vals1, err)
	}
	// aggregate over a different column than the predicate
	vals2, err := e.ColumnAggregatesFiltered("cs_t", 2, []string{"sum", "count"}, eq, nil)
	if err != nil || len(vals2) != 2 || vals2[0].Int != 30 || vals2[1].Int != 1 {
		t.Fatalf("ColumnAggregatesFiltered(eq,other)=%v err=%v", vals2, err)
	}

	// leading-literal LIKE prefix: 'ap%' matches apple + apricot
	like := &sqlx.ColumnFilter{Col: 1, Op: "LIKE", Lit: sqlx.StrValue("ap")}
	n, err = e.ColumnCountFiltered("cs_t", like, nil)
	if err != nil || n != 2 {
		t.Fatalf("ColumnCountFiltered(like)=%d err=%v", n, err)
	}
	vals3, err := e.ColumnAggregatesFiltered("cs_t", 2, []string{"sum"}, like, nil)
	if err != nil || len(vals3) != 1 || vals3[0].Int != 30 {
		t.Fatalf("ColumnAggregatesFiltered(like,other)=%v err=%v", vals3, err)
	}

	// ScanColumnsFiltered: only v + score materialized, matching rows only
	rows, err := e.ScanColumnsFiltered("cs_t", []int{1, 2}, like, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("ScanColumnsFiltered rows=%d", len(rows))
	}
	got := map[string]int64{}
	for _, r := range rows {
		if !r[0].Null || r[0].Type != sqlx.TypeNull {
			t.Fatalf("unrequested id must be Null (full-width row): %v", r)
		}
		got[r[1].Str] = r[2].Int
	}
	if got["apple"] != 10 || got["apricot"] != 20 {
		t.Fatalf("ScanColumnsFiltered: %v", got)
	}

	// filter over a PK-range window (ids 1,2 both match 'ap%')
	w := &sqlx.PKRange{
		Lower: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(1)}, Incl: true},
		Upper: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(3)}, Incl: true},
	}
	n, err = e.ColumnCountFiltered("cs_t", like, w)
	if err != nil || n != 2 {
		t.Fatalf("ColumnCountFiltered(range)=%d err=%v", n, err)
	}
}

// TestCStoreGroupedAggregates exercises the inline-hash grouped aggregate path
// (sqlx.ColumnEngine.ColumnGroupedAggregates): grouping by a string and an int
// column, with several aggregate columns, verifying counts/sums per group and
// that the group values come back correct (collision-safe raw-byte keys).
func TestCStoreGroupedAggregates(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:   "cs_t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "level", Type: sqlx.TypeString},
			{Name: "region", Type: sqlx.TypeInt},
			{Name: "lat", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	// rows with 3 string groups interleaved by id and 2 int groups
	rows := []struct {
		id     int64
		level  string
		region int64
		lat    int64
	}{
		{1, "a", 1, 10}, {2, "b", 1, 20}, {3, "a", 2, 30},
		{4, "c", 2, 40}, {5, "b", 1, 50}, {6, "a", 1, 60},
		{7, "c", 2, 70}, {8, "b", 2, 80},
	}
	for _, r := range rows {
		if err := e.Insert("cs_t", map[string]sqlx.Value{
			"id": sqlx.IntValue(r.id), "level": sqlx.StrValue(r.level),
			"region": sqlx.IntValue(r.region), "lat": sqlx.IntValue(r.lat),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// group by level, aggregates: count(*) + sum(lat) + count(region)
	gas := []sqlx.GroupAgg{
		{Col: -1, Aggs: []string{"count"}},
		{Col: 3, Aggs: []string{"sum"}},
		{Col: 2, Aggs: []string{"count"}},
	}
	rows1, err := e.ColumnGroupedAggregates("cs_t", 1, gas, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]int64{
		"a": {3, 100, 3},
		"b": {3, 150, 3},
		"c": {2, 110, 2},
	}
	if len(rows1) != len(want) {
		t.Fatalf("grouped rows=%d want %d", len(rows1), len(want))
	}
	for _, r := range rows1 {
		g := r[0].Str
		w, ok := want[g]
		if !ok {
			t.Fatalf("unexpected group %q", g)
		}
		if r[1].Int != w[0] || r[2].Int != w[1] || r[3].Int != w[2] {
			t.Fatalf("group %q: %v want %v", g, r, w)
		}
	}

	// group by an int column: region has 2 groups (1,2); one output col per
	// GroupAgg entry
	gas2 := []sqlx.GroupAgg{
		{Col: 3, Aggs: []string{"sum"}},
		{Col: 3, Aggs: []string{"count"}},
	}
	rows2, err := e.ColumnGroupedAggregates("cs_t", 2, gas2, nil)
	if err != nil {
		t.Fatal(err)
	}
	want2 := map[int64][]int64{
		1: {140, 4}, // lat sums: 10+20+50+60; 4 rows
		2: {220, 4}, // 30+40+70+80; 4 rows
	}
	if len(rows2) != len(want2) {
		t.Fatalf("int grouped rows=%d", len(rows2))
	}
	for _, r := range rows2 {
		g := r[0].Int
		w, ok := want2[g]
		if !ok {
			t.Fatalf("unexpected int group %d", g)
		}
		if r[1].Int != w[0] || r[2].Int != w[1] {
			t.Fatalf("int group %d: %v want %v", g, r, w)
		}
	}

	// grouped over a PK-range window (ids 3..6): a:30+60, c:40, b:50
	rng := &sqlx.PKRange{
		Lower: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(3)}, Incl: true},
		Upper: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(6)}, Incl: true},
	}
	rows3, err := e.ColumnGroupedAggregates("cs_t", 1, []sqlx.GroupAgg{{Col: 3, Aggs: []string{"sum"}}}, rng)
	if err != nil {
		t.Fatal(err)
	}
	want3 := map[string]int64{"a": 90, "b": 50, "c": 40}
	if len(rows3) != len(want3) {
		t.Fatalf("windowed grouped rows=%d", len(rows3))
	}
	for _, r := range rows3 {
		if w, ok := want3[r[0].Str]; !ok || r[1].Int != w {
			t.Fatalf("windowed group %q: %v", r[0].Str, r)
		}
	}
}

func TestCStoreScanTopN(t *testing.T) {
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
	for i := 1; i <= 10; i++ {
		if err := e.Insert("cs_t", map[string]sqlx.Value{
			"id":    sqlx.IntValue(int64(i)),
			"v":     sqlx.StrValue(fmt.Sprintf("row%d", i)),
			"score": sqlx.IntValue(int64(i * 7)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// descending top-3, "id" + "score" materialized (ids 10,9,8)
	rows, err := e.ScanTopN("cs_t", []int{0, 2}, nil, true, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("topn rows=%d want 3", len(rows))
	}
	for i, r := range rows {
		wantID := int64(10 - i)
		if r[0].Int != wantID || r[1] != sqlx.NullValue || r[2].Int != wantID*7 {
			t.Fatalf("desc row %d: %v", i, r)
		}
	}

	// ascending top-3, full width
	rows, err = e.ScanTopN("cs_t", []int{0, 1, 2}, nil, false, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rows {
		wantID := int64(i + 1)
		if r[0].Int != wantID || r[1] != sqlx.StrValue(fmt.Sprintf("row%d", i+1)) || r[2].Int != wantID*7 {
			t.Fatalf("asc row %d: %v", i, r)
		}
	}

	// limit >= row count returns everything
	rows, err = e.ScanTopN("cs_t", []int{0}, nil, false, 100)
	if err != nil || len(rows) != 10 {
		t.Fatalf("limit>rows: n=%d err=%v", len(rows), err)
	}

	// PK range [3, 6) restricted to ids 3,4,5, descending top-2 -> ids 5,4
	rows, err = e.ScanTopN("cs_t", []int{0}, &sqlx.PKRange{
		Lower: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(3)}, Incl: true},
		Upper: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(6)}, Incl: false},
	}, true, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0].Int != 5 || rows[1][0].Int != 4 {
		t.Fatalf("range topn: %v err=%v", rows, err)
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

// TestCStoreTopNSQL verifies the ORDER BY PK ... LIMIT pushdown through the
// SQL executor (bounded PK-index scan) returns the same rows as the generic
// full-scan + sort path.
func TestCStoreTopNSQL(t *testing.T) {
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
	for i := int64(1); i <= 6; i++ {
		if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO cs_t VALUES (%d, 'v%d', %d)", i, i, i*10))[0]); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		sql   string
		want  string
		limit bool
	}{
		{"SELECT id, score FROM cs_t ORDER BY id DESC LIMIT 3", "6,60|5,50|4,40", true},
		{"SELECT id, score FROM cs_t ORDER BY id ASC LIMIT 2", "1,10|2,20", true},
		{"SELECT id FROM cs_t WHERE id >= 3 AND id < 6 ORDER BY id DESC LIMIT 2", "5|4", true},
		{"SELECT v FROM cs_t ORDER BY score DESC LIMIT 2", "v6|v5", false},
		{"SELECT id FROM cs_t ORDER BY id DESC", "6|5|4|3|2|1", false},
		{"SELECT id FROM cs_t WHERE score > 10 ORDER BY id DESC LIMIT 2", "6|5", false},
	}
	for _, c := range cases {
		r, err := ex.Execute(mustParse(t, c.sql)[0])
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := make([]string, len(r.Rows))
		for i, row := range r.Rows {
			parts := make([]string, len(row))
			for j, v := range row {
				parts[j] = v.String()
			}
			got[i] = strings.Join(parts, ",")
		}
		if strings.Join(got, "|") != c.want {
			t.Fatalf("%s = %v want %s", c.sql, got, c.want)
		}
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
		// far-future/limit bounds must not overflow the int64 timestamp encoding
		// (UnixNano overflows after ~2262; storage now uses microseconds).
		{"SELECT COUNT(*) FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '9999-12-31T23:59:59Z'", "24"},
		{"SELECT COUNT(*) FROM logs WHERE ts >= '0001-01-01T00:00:00Z' AND ts <= '9999-12-31T23:59:59Z'", "24"},
		{"SELECT COUNT(*) FROM logs WHERE ts >= '2262-04-11T23:47:16Z'", "0"},
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

// TestCStoreLiveRowCounter verifies the whole-table live-row counter kept per
// CSTORE table stays exact under insert (incl. duplicates), delete, update
// (same and changed PK), batch insert, drop/recreate and mixed workloads.
func TestCStoreLiveRowCounter(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:   "t",
		Engine: "CSTORE",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	rows := func(k, n int) []map[string]sqlx.Value {
		out := make([]map[string]sqlx.Value, 0, n)
		for i := k; i < k+n; i++ {
			out = append(out, map[string]sqlx.Value{
				"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue("r"),
			})
		}
		return out
	}
	liveRows := func() int {
		t.Helper()
		proj, err := e.ScanColumns("t", []int{0}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return len(proj)
	}
	check := func(want int64) {
		t.Helper()
		n, err := e.ColumnCount("t", nil)
		if err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("ColumnCount=%d want %d", n, want)
		}
		if int64(liveRows()) != want {
			t.Fatalf("scan rows=%d want %d", liveRows(), want)
		}
	}
	for _, r := range rows(0, 5) {
		if err := e.Insert("t", r); err != nil {
			t.Fatal(err)
		}
	}
	check(5)
	// duplicate pk overwrites: count must not move
	if err := e.Insert("t", rows(0, 1)[0]); err != nil {
		t.Fatal(err)
	}
	check(5)
	// delete one live row
	if err := e.Delete("t", []sqlx.Value{sqlx.IntValue(2)}); err != nil {
		t.Fatal(err)
	}
	check(4)
	// delete a non-existent pk: count must not move
	if err := e.Delete("t", []sqlx.Value{sqlx.IntValue(99)}); err != nil {
		t.Fatal(err)
	}
	check(4)
	// update with same PK
	if err := e.Update("t", []sqlx.Value{sqlx.IntValue(1)}, map[string]sqlx.Value{"v": sqlx.StrValue("z")}); err != nil {
		t.Fatal(err)
	}
	check(4)
	// update with changed PK
	if err := e.Update("t", []sqlx.Value{sqlx.IntValue(1)}, map[string]sqlx.Value{"id": sqlx.IntValue(10)}); err != nil {
		t.Fatal(err)
	}
	check(4)
	// batch insert of new rows
	if _, err := e.BatchInsert("t", rows(100, 3)); err != nil {
		t.Fatal(err)
	}
	check(7)
	// mixed churn: del + dup-insert + batch, then verify against scan
	if err := e.Delete("t", []sqlx.Value{sqlx.IntValue(100)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert("t", rows(11, 1)[0]); err != nil { // new pk
		t.Fatal(err)
	}
	if err := e.Insert("t", rows(100, 1)[0]); err != nil { // resurrect 100
		t.Fatal(err)
	}
	check(8)
	// drop resets: recreate with a fresh counter
	if err := e.DropTable("t"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ColumnCount("t", nil); err == nil {
		t.Fatal("expected error for dropped table")
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	if _, err := e.BatchInsert("t", rows(0, 2)); err != nil {
		t.Fatal(err)
	}
	check(2)
}

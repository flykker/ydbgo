package storage

import (
	"testing"

	sqlx "ydbgo/internal/sql"
)

func indexTestSchema(name string) *sqlx.TableSchema {
	return &sqlx.TableSchema{
		Name: name,
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "name", Type: sqlx.TypeString},
			{Name: "age", Type: sqlx.TypeInt},
		},
		PK:     []string{"id"},
		Engine: "CSTORE",
	}
}

func mustIndexTable(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.CreateTable(indexTestSchema("t")); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]sqlx.Value{
		{"id": sqlx.IntValue(1), "name": sqlx.StrValue("Alice"), "age": sqlx.IntValue(30)},
		{"id": sqlx.IntValue(2), "name": sqlx.StrValue("Bob"), "age": sqlx.IntValue(25)},
		{"id": sqlx.IntValue(3), "name": sqlx.StrValue("Bobby"), "age": sqlx.IntValue(40)},
		{"id": sqlx.IntValue(4), "name": sqlx.StrValue("Cara"), "age": sqlx.IntValue(30)},
	}
	for _, r := range rows {
		if err := e.Insert("t", r); err != nil {
			t.Fatal(err)
		}
	}
}

func eqPred(col int, lit sqlx.Value) *sqlx.ColumnFilter {
	return &sqlx.ColumnFilter{Col: col, Op: "=", Lit: lit}
}

func likePred(col int, prefix string) *sqlx.ColumnFilter {
	return &sqlx.ColumnFilter{Col: col, Op: "LIKE", Lit: sqlx.StrValue(prefix)}
}

func TestIndexCreateBackfillAndEquality(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	mustIndexTable(t, e)

	if err := e.CreateIndex("t", "ix_name", []string{"name"}, false); err != nil {
		t.Fatal(err)
	}

	n, err := e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Bob")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Bob count=%d want 1", n)
	}

	n, err = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Nobody")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("missing count=%d want 0", n)
	}

	rows, err := e.ScanColumnsFiltered("t", []int{0, 1}, eqPred(2, sqlx.IntValue(30)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("age=30 rows=%d want 2", len(rows))
	}

	if err := e.DropIndex("t", "ix_name", false); err != nil {
		t.Fatal(err)
	}
	if err := e.DropIndex("t", "ix_name", true); err != nil {
		t.Fatalf("drop if-exists after drop should succeed: %v", err)
	}
	if err := e.DropIndex("t", "ix_name", false); err == nil {
		t.Fatal("dropping a missing index without IF EXISTS should fail")
	}
}

func TestIndexLike(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	mustIndexTable(t, e)
	if err := e.CreateIndex("t", "ix_name", []string{"name"}, false); err != nil {
		t.Fatal(err)
	}

	n, err := e.ColumnCountFiltered("t", likePred(1, "Bob"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 { // Bob, Bobby
		t.Fatalf("Bob%% count=%d want 2", n)
	}

	n, err = e.ColumnCountFiltered("t", likePred(1, "B"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("B%% count=%d want 2", n)
	}
}

func TestIndexDMLMaintenance(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	mustIndexTable(t, e)
	if err := e.CreateIndex("t", "ix_name", []string{"name"}, false); err != nil {
		t.Fatal(err)
	}

	// Update re-indexes: Bob -> Zara
	if err := e.Update("t", []sqlx.Value{sqlx.IntValue(2)}, map[string]sqlx.Value{"name": sqlx.StrValue("Zara")}); err != nil {
		t.Fatal(err)
	}
	n, _ := e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Bob")), nil)
	if n != 0 {
		t.Fatalf("Bob after update=%d want 0", n)
	}
	n, _ = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Zara")), nil)
	if n != 1 {
		t.Fatalf("Zara after update=%d want 1", n)
	}

	// Overwrite (Put) re-indexes: Alice -> Anna
	if err := e.PutBySQL("t", map[string]sqlx.Value{"id": sqlx.IntValue(1), "name": sqlx.StrValue("Anna"), "age": sqlx.IntValue(31)}); err != nil {
		t.Fatal(err)
	}
	n, _ = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Alice")), nil)
	if n != 0 {
		t.Fatalf("Alice after put=%d want 0", n)
	}
	n, _ = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Anna")), nil)
	if n != 1 {
		t.Fatalf("Anna after put=%d want 1", n)
	}

	// Delete removes the entry.
	if err := e.Delete("t", []sqlx.Value{sqlx.IntValue(3)}); err != nil {
		t.Fatal(err)
	}
	n, _ = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Bobby")), nil)
	if n != 0 {
		t.Fatalf("Bobby after delete=%d want 0", n)
	}

	// DeleteRange removes entries too.
	if _, err := e.DeleteRange("t", &sqlx.PKRange{Lower: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(1)}, Incl: true}, Upper: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(4)}, Incl: true}}); err != nil {
		t.Fatal(err)
	}
	n, _ = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Anna")), nil)
	if n != 0 {
		t.Fatalf("Anna after range delete=%d want 0", n)
	}
	n, _ = e.ColumnCountFiltered("t", eqPred(1, sqlx.StrValue("Zara")), nil)
	if n != 0 {
		t.Fatalf("Zara after range delete=%d want 0", n)
	}
}

func TestIndexPersistenceAndRebuild(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustIndexTable(t, e)
	if err := e.CreateIndex("t", "ix_age", []string{"age"}, false); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	// Entries are rebuilt on open, so the equality fast path still works.
	n, err := e2.ColumnCountFiltered("t", eqPred(2, sqlx.IntValue(30)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("age=30 after reopen=%d want 2", n)
	}

	// DML after reopen keeps the rebuilt entries in sync.
	if err := e2.Update("t", []sqlx.Value{sqlx.IntValue(4)}, map[string]sqlx.Value{"age": sqlx.IntValue(99)}); err != nil {
		t.Fatal(err)
	}
	n, _ = e2.ColumnCountFiltered("t", eqPred(2, sqlx.IntValue(30)), nil)
	if n != 1 {
		t.Fatalf("age=30 after update=%d want 1", n)
	}
	n, _ = e2.ColumnCountFiltered("t", eqPred(2, sqlx.IntValue(99)), nil)
	if n != 1 {
		t.Fatalf("age=99 after update=%d want 1", n)
	}
}

func TestIndexErrors(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	mustIndexTable(t, e)

	if err := e.CreateIndex("t", "ix", []string{"name"}, false); err != nil {
		t.Fatal(err)
	}
	if err := e.CreateIndex("t", "ix", []string{"name"}, false); err == nil {
		t.Fatal("duplicate index without IF NOT EXISTS should fail")
	}
	if err := e.CreateIndex("t", "ix", []string{"name"}, true); err != nil {
		t.Fatalf("duplicate index with IF NOT EXISTS should no-op: %v", err)
	}
	if err := e.CreateIndex("t", "ix2", []string{"name", "age"}, false); err == nil {
		t.Fatal("composite index should be rejected")
	}
	if err := e.CreateIndex("t", "ix3", []string{"nope"}, false); err == nil {
		t.Fatal("unknown index column should fail")
	}
	if err := e.CreateIndex("missing", "ix", []string{"name"}, false); err == nil {
		t.Fatal("index on a missing table should fail")
	}
}

func TestIndexAggregates(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	mustIndexTable(t, e)
	if err := e.CreateIndex("t", "ix_name", []string{"name"}, false); err != nil {
		t.Fatal(err)
	}

	vals, err := e.ColumnAggregatesFiltered("t", 2, []string{"count", "sum", "avg"}, eqPred(1, sqlx.StrValue("Bobby")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if vals[0].Int != 1 || vals[1].Int != 40 || vals[2].Flt != 40 {
		t.Fatalf("aggs=%v want count=1 sum=40 avg=40", vals)
	}

	vals, err = e.ColumnAggregatesFiltered("t", 2, []string{"count", "sum", "avg"}, eqPred(1, sqlx.StrValue("Missing")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if vals[0].Int != 0 {
		t.Fatalf("empty aggs=%v want count=0", vals)
	}
}

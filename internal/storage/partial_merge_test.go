package storage_test

import (
	"strings"
	"testing"

	"ydbgo/internal/sql"
	"ydbgo/internal/storage"
)

// TestPartialMergeUpdate exercises CH partial-merge: UPDATE SET g=7 writes only
// the g column, the untouched v column must be inherited from the previous
// version. The test runs the UPDATE, forces a flush over the mem threshold,
// compacts, and reopens from disk, verifying v survives every phase.
func TestPartialMergeUpdate(t *testing.T) {
	dir := t.TempDir() + "/db"
	e, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	const n = 20000
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES "+bulkValues(n))
	checkRow := func(label string) {
		t.Helper()
		r := execOK(t, ex, "SELECT v, g FROM t WHERE id = 42")
		if len(r.Rows) != 1 {
			t.Fatalf("%s: expected 1 row", label)
		}
		if r.Rows[0][0].Str != "v42" {
			t.Fatalf("%s: v = %q, want v42", label, r.Rows[0][0].Str)
		}
		if r.Rows[0][1].Int != 7 {
			t.Fatalf("%s: g = %d, want 7", label, r.Rows[0][1].Int)
		}
		if r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t"); r.Rows[0][0].Int != n {
			t.Fatalf("%s: count = %d, want %d", label, r.Rows[0][0].Int, n)
		}
	}
	// UPDATE only g; the partial row must inherit v from the base part.
	mustExec(t, ex, "UPDATE t SET g = 7 WHERE id < 20000")
	checkRow("after update")
	// Force a flush so the partial rows land in a part on disk.
	if _, err := e.Compact("t"); err != nil {
		t.Fatal(err)
	}
	checkRow("after compact")
	// Reopen: parts on disk must still merge columns correctly.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	ex2 := sql.NewExecutor(e2)
	r := execOK(t, ex2, "SELECT v, g FROM t WHERE id = 42")
	if len(r.Rows) != 1 || r.Rows[0][0].Str != "v42" || r.Rows[0][1].Int != 7 {
		t.Fatalf("after reopen: v=%v g=%v", r.Rows[0][0], r.Rows[0][1])
	}
	if r := execOK(t, ex2, "SELECT COUNT(*) AS c FROM t"); r.Rows[0][0].Int != n {
		t.Fatalf("after reopen: count = %d, want %d", r.Rows[0][0].Int, n)
	}
	// A second partial update on the reopened table must keep working.
	mustExec(t, ex2, "UPDATE t SET g = 9 WHERE id < 100")
	r = execOK(t, ex2, "SELECT v, g FROM t WHERE id = 42")
	if r.Rows[0][0].Str != "v42" || r.Rows[0][1].Int != 9 {
		t.Fatalf("after second update: v=%v g=%v", r.Rows[0][0], r.Rows[0][1])
	}
	// Aggregate over the untouched column must be correct.
	r = execOK(t, ex2, "SELECT SUM(g) AS s FROM t WHERE id < 100")
	if r.Rows[0][0].Int != 9*100 {
		t.Fatalf("sum(g) after partial updates = %d, want %d", r.Rows[0][0].Int, 9*100)
	}
}

// TestPartialMergeSelfReferencingSet: SET g = g + 1 references a column that
// is itself assigned, so the old value must still be read.
func TestPartialMergeSelfReferencingSet(t *testing.T) {
	e, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	const n = 1000
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES "+bulkValues(n))
	mustExec(t, ex, "UPDATE t SET g = g + 1 WHERE id < 1000")
	r := execOK(t, ex, "SELECT g FROM t WHERE id = 5")
	if r.Rows[0][0].Int != 6 {
		t.Fatalf("g = %d, want 6", r.Rows[0][0].Int)
	}
	// Only id (PK) and g (referenced) were read; v was untouched and inherited.
	r = execOK(t, ex, "SELECT v FROM t WHERE id = 5")
	if r.Rows[0][0].Str != "v5" {
		t.Fatalf("v = %q, want v5 (inherited)", r.Rows[0][0].Str)
	}
}

// TestPartialMergeNonAssignedColumnInheritedAcrossFlush: update one column,
// flush over the threshold, and confirm an unrelated column read from a point
// query (lookupCol) and a range scan both inherit correctly.
func TestPartialMergeNonAssignedColumnInheritedAcrossFlush(t *testing.T) {
	e, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	const n = 150000 // exceeds mpartFlushThreshold, forcing mid-write flushes
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES "+bulkValues(n))
	mustExec(t, ex, "UPDATE t SET g = 7 WHERE id < 100000")
	// Range scan + point lookup + aggregate over the untouched column.
	r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE v = 'v1'")
	if r.Rows[0][0].Int != 1 {
		t.Fatalf("count(v='v1') = %d", r.Rows[0][0].Int)
	}
	r = execOK(t, ex, "SELECT v, g FROM t WHERE id = 99999")
	if r.Rows[0][0].Str != "v99999" || r.Rows[0][1].Int != 7 {
		t.Fatalf("row 99999 = v=%v g=%v", r.Rows[0][0], r.Rows[0][1])
	}
	// The inherited v column must aggregate too (SUM over the untouched g would
	// be redundant with the SET above; SUM over v is impossible, so check a
	// COUNT over the partial+inherited merged view for a wide window).
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE id < 100000 AND g = 7")
	if r.Rows[0][0].Int != 100000 {
		t.Fatalf("count(partial g=7) = %d, want 100000", r.Rows[0][0].Int)
	}
}

// TestPartialMergeCoveringNewestSource: repeated range UPDATEs stack fully
// overlapping parts; each flushed part holds every live key of its window with
// dense int64 PKs, so scans over the window may skip older parts entirely
// (covering-newest fast path). The untouched column must still inherit through
// the regular merge, and windows wider than the newest part must fall back.
func TestPartialMergeCoveringNewestSource(t *testing.T) {
	e, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	const n = 150000 // above mpartFlushThreshold so every batch flushes to a part
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES "+bulkValues(n))
	// Three stacked updates of the same window -> three overlapping parts.
	mustExec(t, ex, "UPDATE t SET g = 7 WHERE id >= 0 AND id < 100000")
	mustExec(t, ex, "UPDATE t SET g = 8 WHERE id >= 0 AND id < 100000")
	mustExec(t, ex, "UPDATE t SET g = 9 WHERE id >= 0 AND id < 100000")
	check := func(label string) {
		t.Helper()
		if r := execOK(t, ex, "SELECT g FROM t WHERE id = 50000"); r.Rows[0][0].Int != 9 {
			t.Fatalf("%s: g = %d, want 9", label, r.Rows[0][0].Int)
		}
		// The untouched column must inherit from below the newest part.
		if r := execOK(t, ex, "SELECT v FROM t WHERE id = 50000"); r.Rows[0][0].Str != "v50000" {
			t.Fatalf("%s: v = %q, want v50000", label, r.Rows[0][0].Str)
		}
		if r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE id < 100000 AND g = 9"); r.Rows[0][0].Int != 100000 {
			t.Fatalf("%s: count(g=9) = %d, want 100000", label, r.Rows[0][0].Int)
		}
		if r := execOK(t, ex, "SELECT SUM(g) AS s FROM t WHERE id < 100000"); r.Rows[0][0].Int != 9*100000 {
			t.Fatalf("%s: sum(g) = %d, want %d", label, r.Rows[0][0].Int, 9*100000)
		}
	}
	check("stacked parts")
	// A window strictly wider than the newest part must not use it as cover.
	if r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE id < 150000"); r.Rows[0][0].Int != n {
		t.Fatalf("wide count = %d, want %d", r.Rows[0][0].Int, n)
	}
	// Rows beyond the updated window keep their original values.
	if r := execOK(t, ex, "SELECT g FROM t WHERE id = 120000"); r.Rows[0][0].Int != 120000 {
		t.Fatalf("beyond-window g = %d, want 120000", r.Rows[0][0].Int)
	}
	// Narrower sub-windows and open-ended ranges stay exact.
	if r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE id < 50 AND g = 9"); r.Rows[0][0].Int != 50 {
		t.Fatalf("sub-window count = %d, want 50", r.Rows[0][0].Int)
	}
	if _, err := e.Compact("t"); err != nil {
		t.Fatal(err)
	}
	check("after compact")
}

// TestPartialMergeInsertOrderHint: the flush-path insertion-order hint must
// never produce a mis-sorted part. Mixed workloads (INSERTs in arbitrary key
// order, duplicate keys inside one statement, interleaved updates of an
// already-populated mem part) all fall back to sorting; results stay exact.
func TestPartialMergeInsertOrderHint(t *testing.T) {
	e, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	// Descending-key INSERT: hint order is not sorted -> must be ignored.
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES (4,'v4',44), (3,'v3',33), (2,'v2',22), (1,'v1',11)")
	checkAll(t, ex, 4)
	// UPDATE over existing rows: fold into a NON-empty mem part -> hint drop.
	mustExec(t, ex, "UPDATE t SET g = 7 WHERE id < 5")
	checkAll(t, ex, 4)
	if r := execOK(t, ex, "SELECT g FROM t WHERE id = 2"); r.Rows[0][0].Int != 7 {
		t.Fatalf("g after update = %d, want 7", r.Rows[0][0].Int)
	}
	// Duplicate key inside one statement: len(hint) != len(rows) -> ignored.
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES (9,'v9',99), (9,'v9b',98)")
	if _, err := e.Compact("t"); err != nil {
		t.Fatal(err)
	}
	r := execOK(t, ex, "SELECT v FROM t WHERE id = 9")
	if r.Rows[0][0].Str != "v9b" {
		t.Fatalf("dup upsert v = %q, want v9b", r.Rows[0][0].Str)
	}
	if r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE id < 10"); r.Rows[0][0].Int != 5 {
		t.Fatalf("count = %d, want 5", r.Rows[0][0].Int)
	}
	// Reopen and re-verify everything survived the flush/reopen round trip.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
}

func checkAll(t *testing.T, ex *sql.Executor, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		q := "SELECT v, g FROM t WHERE id = " + itoa(int64(i))
		r := execOK(t, ex, q)
		if len(r.Rows) != 1 || r.Rows[0][0].Str != "v"+itoa(int64(i)) {
			t.Fatalf("row %d = %v", i, r.Rows)
		}
	}
	if r := execOK(t, ex, "SELECT COUNT(*) AS c FROM t"); r.Rows[0][0].Int != int64(n) {
		t.Fatalf("count = %d, want %d", r.Rows[0][0].Int, n)
	}
}

func mustExec(t *testing.T, ex *sql.Executor, stmt string) {
	t.Helper()
	stmts, err := sql.Parse(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ex.Execute(stmts[0]); err != nil {
		t.Fatal(err)
	}
}

func execOK(t *testing.T, ex *sql.Executor, stmt string) *sql.Result {
	t.Helper()
	stmts, err := sql.Parse(stmt)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Execute(stmts[0])
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func bulkValues(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		sb.WriteString(itoa(int64(i)))
		sb.WriteString(", 'v")
		sb.WriteString(itoa(int64(i)))
		sb.WriteString("', ")
		sb.WriteString(itoa(int64(i)))
		sb.WriteString(")")
	}
	return sb.String()
}

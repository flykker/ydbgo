package storage_test

import (
	"fmt"
	"testing"

	"ydbgo/internal/sql"
	"ydbgo/internal/storage"
)

// TestVectorizedConstRangeUpdate exercises the constant-SET UPDATE fast path:
// the storage streams the merged PK column and shares one encoded cell per
// assigned column across every rewritten key (no row materialization).
// Covers affected counts, inheritance of untouched columns, tombstone
// skipping, NULL assignment, multi-column constants and persistence across
// compact + reopen.
func TestVectorizedConstRangeUpdate(t *testing.T) {
	dir := t.TempDir() + "/db"
	e, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	const n = 5000 // g starts equal to id
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES "+bulkValues(n))

	row := func(id int64) *sql.Result {
		return execOK(t, ex, fmt.Sprintf("SELECT v, g FROM t WHERE id = %d", id))
	}

	// Constant range update rewrites exactly the live rows of the window;
	// the upper bound is exclusive.
	r := execOK(t, ex, "UPDATE t SET g = 42 WHERE id >= 100 AND id < 200")
	if r.Affected != 100 {
		t.Fatalf("affected = %d, want 100", r.Affected)
	}
	if rr := row(150); rr.Rows[0][0].Str != "v150" || rr.Rows[0][1].Int != 42 {
		t.Fatalf("inside window: v=%v g=%v", rr.Rows[0][0], rr.Rows[0][1])
	}
	if rr := row(50); rr.Rows[0][1].Int != 50 {
		t.Fatalf("outside window: g = %d, want 50", rr.Rows[0][1].Int)
	}
	if rr := row(200); rr.Rows[0][1].Int != 200 {
		t.Fatalf("upper bound not exclusive: g = %d", rr.Rows[0][1].Int)
	}

	// Multi-column constants: each assigned column shares its cell.
	r = execOK(t, ex, "UPDATE t SET g = 77, v = 'z' WHERE id >= 300 AND id < 305")
	if r.Affected != 5 {
		t.Fatalf("multi-col affected = %d, want 5", r.Affected)
	}
	if rr := row(302); rr.Rows[0][0].Str != "z" || rr.Rows[0][1].Int != 77 {
		t.Fatalf("multi-col: v=%v g=%v", rr.Rows[0][0], rr.Rows[0][1])
	}

	// Deleted rows inside the window are skipped, not resurrected.
	mustExec(t, ex, "DELETE FROM t WHERE id = 120")
	r = execOK(t, ex, "UPDATE t SET g = 43 WHERE id >= 100 AND id < 200")
	if r.Affected != 99 {
		t.Fatalf("affected after delete = %d, want 99", r.Affected)
	}
	if rr := execOK(t, ex, "SELECT COUNT(*) AS c FROM t WHERE id = 120"); rr.Rows[0][0].Int != 0 {
		t.Fatalf("deleted row resurrected: count = %d", rr.Rows[0][0].Int)
	}
	if rr := row(119); rr.Rows[0][1].Int != 43 {
		t.Fatalf("neighbor of tombstone: g = %d, want 43", rr.Rows[0][1].Int)
	}

	// NULL literal assignment writes a null cell.
	r = execOK(t, ex, "UPDATE t SET g = NULL WHERE id < 3")
	if r.Affected != 3 {
		t.Fatalf("null affected = %d, want 3", r.Affected)
	}
	if rr := row(1); !rr.Rows[0][1].Null {
		t.Fatalf("g after NULL set = %v, want NULL", rr.Rows[0][1])
	}

	// The rewritten rows survive flush + compact + reopen with inheritance.
	if _, err := e.Compact("t"); err != nil {
		t.Fatal(err)
	}
	if rr := row(150); rr.Rows[0][0].Str != "v150" || rr.Rows[0][1].Int != 43 {
		t.Fatalf("after compact: v=%v g=%v", rr.Rows[0][0], rr.Rows[0][1])
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	ex2 := sql.NewExecutor(e2)
	rr := execOK(t, ex2, "SELECT COUNT(*) AS c FROM t")
	if rr.Rows[0][0].Int != n-1 {
		t.Fatalf("after reopen: count = %d, want %d", rr.Rows[0][0].Int, n-1)
	}
	rr = execOK(t, ex2, "SELECT v, g FROM t WHERE id = 150")
	if rr.Rows[0][0].Str != "v150" || rr.Rows[0][1].Int != 43 {
		t.Fatalf("after reopen: v=%v g=%v", rr.Rows[0][0], rr.Rows[0][1])
	}
	rr = execOK(t, ex2, "SELECT SUM(g) AS s FROM t WHERE id >= 300 AND id < 305")
	if rr.Rows[0][0].Int != 77*5 {
		t.Fatalf("sum over const cells = %d, want %d", rr.Rows[0][0].Int, 77*5)
	}
}

// TestVectorizedConstRangeUpdateStringPK: tables outside the fast path's
// shape (non-int PK) fall back to the generic scan-and-rewrite path and stay
// correct.
func TestVectorizedConstRangeUpdateStringPK(t *testing.T) {
	e, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE s (k string primary key, g int64) ENGINE=CSTORE2")
	mustExec(t, ex, "INSERT INTO s (k, g) VALUES ('a', 1), ('b', 2), ('m', 13), ('z', 26)")
	r := execOK(t, ex, "UPDATE s SET g = 5 WHERE k < 'm'")
	if r.Affected != 2 {
		t.Fatalf("affected = %d, want 2", r.Affected)
	}
	rr := execOK(t, ex, "SELECT g FROM s WHERE k = 'b'")
	if rr.Rows[0][0].Int != 5 {
		t.Fatalf("g(b) = %d, want 5", rr.Rows[0][0].Int)
	}
	rr = execOK(t, ex, "SELECT g FROM s WHERE k = 'm'")
	if rr.Rows[0][0].Int != 13 {
		t.Fatalf("g(m) = %d, want 13", rr.Rows[0][0].Int)
	}
}

// TestVectorizedConstRangeUpdateAggregateInMem: a constant rewrite whose rows
// are still unflushed (below the mem threshold) leaves partial rows in the
// mem part; numeric aggregates over that window must inherit untouched
// columns from older versions, never read fabricated zeros.
func TestVectorizedConstRangeUpdateAggregateInMem(t *testing.T) {
	e, err := storage.Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sql.NewExecutor(e)
	mustExec(t, ex, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2")
	const n = 500 // far below mpartFlushThreshold: everything stays in mem
	mustExec(t, ex, "INSERT INTO t (id, v, g) VALUES "+bulkValues(n))
	mustExec(t, ex, "UPDATE t SET g = 5 WHERE id >= 100 AND id < 200")
	// SUM over the rewritten window reads the PK column of PARTIAL rows (the
	// rewrite leaves id/v cells empty): inheritance must yield real ids, not
	// fabricated zeros.
	r := execOK(t, ex, "SELECT SUM(id) AS s FROM t WHERE id >= 100 AND id < 200")
	if r.Rows[0][0].Int != 14950 {
		t.Fatalf("sum(id) in-mem = %d, want 14950", r.Rows[0][0].Int)
	}
	// SUM over a window mixing partial and untouched rows.
	r = execOK(t, ex, "SELECT SUM(g) AS s FROM t WHERE id < 300")
	want := int64(0)
	for i := 0; i < 300; i++ {
		if i >= 100 && i < 200 {
			want += 5
		} else {
			want += int64(i)
		}
	}
	if r.Rows[0][0].Int != want {
		t.Fatalf("sum(g) mixed = %d, want %d", r.Rows[0][0].Int, want)
	}
}

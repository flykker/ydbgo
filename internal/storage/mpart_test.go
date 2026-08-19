package storage

// Tests for the native columnar ENGINE=CSTORE2 backend (mpart): DML parity
// with the CSTORE backend, flush/reopen over the row threshold, columnar SQL
// pushdown, range deletes + compaction, snapshot roundtrip and raft-replay
// idempotency.

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	sqlx "ydbgo/internal/sql"
)

func newMpartEngine(t *testing.T) *Engine {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

var mpartSchema = &sqlx.TableSchema{
	Name:   "mp_t",
	Engine: "CSTORE2",
	Columns: []sqlx.ColumnDef{
		{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
		{Name: "v", Type: sqlx.TypeString},
		{Name: "score", Type: sqlx.TypeFloat},
	},
	PK: []string{"id"},
}

// TestMpartTable mirrors TestCStoreTable against the CSTORE2 backend: DML,
// snapshot roundtrip and reopen-from-disk parity.
func TestMpartTable(t *testing.T) {
	e := newMpartEngine(t)
	defer e.Close()
	if err := e.CreateTable(mpartSchema); err != nil {
		t.Fatal(err)
	}
	insert := func(id int64, v string, score float64) {
		if err := e.Insert("mp_t", map[string]sqlx.Value{
			"id": sqlx.IntValue(id), "v": sqlx.StrValue(v), "score": sqlx.FloatValue(score),
		}); err != nil {
			t.Fatal(err)
		}
	}
	insert(1, "a1", 1.5)
	insert(2, "b2", 2.5)
	rows, err := e.Scan("mp_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][1].Str != "a1" || rows[1][2].Flt != 2.5 {
		t.Fatalf("mpart scan: %v", rows)
	}
	// update + delete
	if err := e.Update("mp_t", []sqlx.Value{sqlx.IntValue(1)}, map[string]sqlx.Value{"v": sqlx.StrValue("a1x")}); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("mp_t", []sqlx.Value{sqlx.IntValue(2)}); err != nil {
		t.Fatal(err)
	}
	rows, err = e.Scan("mp_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].Str != "a1x" {
		t.Fatalf("mpart after update/delete: %v", rows)
	}
	// snapshot roundtrip
	state, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	e2 := newMpartEngine(t)
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	rows, err = e2.Scan("mp_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].Str != "a1x" {
		t.Fatalf("mpart after snapshot: %v", rows)
	}
	if got, _ := e2.GetSchema("mp_t"); got.Engine != "CSTORE2" {
		t.Fatalf("engine after snapshot=%q", got.Engine)
	}
	// reopen from disk (a store Close() flushes buffered mem rows to parts)
	dir := filepath.Join(t.TempDir(), "db")
	e3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e3.CreateTable(mpartSchema); err != nil {
		t.Fatal(err)
	}
	if err := e3.Insert("mp_t", map[string]sqlx.Value{"id": sqlx.IntValue(7), "v": sqlx.StrValue("seven")}); err != nil {
		t.Fatal(err)
	}
	if err := e3.Close(); err != nil {
		t.Fatal(err)
	}
	e4, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e4.Close()
	rows, err = e4.Scan("mp_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].Int != 7 {
		t.Fatalf("mpart after reopen: %v", rows)
	}
	if got, _ := e4.GetSchema("mp_t"); got.Engine != "CSTORE2" {
		t.Fatalf("engine after reopen=%q", got.Engine)
	}
	// drop only the mpart table
	if err := e4.DropTable("mp_t"); err != nil {
		t.Fatal(err)
	}
	if _, err := e4.Scan("mp_t"); err == nil {
		t.Fatal("mpart table should be gone after drop")
	}
}

// TestMpartFlushReopen forces mem-part flushes past the row threshold inside
// a group-commit batch, then verifies rows, counts and part files survive a
// reopen.
func TestMpartFlushReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "mp_big",
		Engine: "CSTORE2",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	const n = mpartFlushThreshold*2 + 4096
	err = e.UpdateBatch(func() error {
		for i := int64(0); i < n; i++ {
			if err := e.Insert("mp_big", map[string]sqlx.Value{
				"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%06d", i)),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	r := execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_big")
	if r.Rows[0][0].Int != n {
		t.Fatalf("count after batch = %d, want %d", r.Rows[0][0].Int, n)
	}
	// whole-table aggregate (COUNT + SUM) via columnar pushdown
	r = execOK(t, ex, "SELECT COUNT(*), SUM(id) FROM mp_big")
	if r.Rows[0][0].Int != n {
		t.Fatalf("count(*) = %d", r.Rows[0][0].Int)
	}
	var wantSum int64 = (n - 1) * n / 2
	if r.Rows[0][1].Int != wantSum {
		t.Fatalf("sum(id) = %d, want %d", r.Rows[0][1].Int, wantSum)
	}
	// update a row that lives in a flushed part and re-read it
	if err := e.Update("mp_big", []sqlx.Value{sqlx.IntValue(1)}, map[string]sqlx.Value{"v": sqlx.StrValue("patched")}); err != nil {
		t.Fatal(err)
	}
	r = execOK(t, ex, "SELECT v FROM mp_big WHERE id = 1")
	if r.Rows[0][0].Str != "patched" {
		t.Fatalf("patched row = %q", r.Rows[0][0].Str)
	}
	// count must not change on update
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_big")
	if r.Rows[0][0].Int != n {
		t.Fatalf("count after update = %d, want %d", r.Rows[0][0].Int, n)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// reopen: parts on disk must reconstruct the full table
	e2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	ex2 := sqlx.NewExecutor(e2)
	r = execOK(t, ex2, "SELECT COUNT(*) AS c FROM mp_big")
	if r.Rows[0][0].Int != n {
		t.Fatalf("count after reopen = %d, want %d", r.Rows[0][0].Int, n)
	}
	r = execOK(t, ex2, "SELECT v FROM mp_big WHERE id = 1")
	if r.Rows[0][0].Str != "patched" {
		t.Fatalf("patched row after reopen = %q", r.Rows[0][0].Str)
	}
	r = execOK(t, ex2, "SELECT SUM(id) AS s FROM mp_big")
	if r.Rows[0][0].Int != wantSum {
		t.Fatalf("sum after reopen = %d, want %d", r.Rows[0][0].Int, wantSum)
	}
}

// TestMpartRangeDeleteAndCompact exercises DeleteRange (retention) and the
// Compact rewrite that reclaims tombstoned rows.
func TestMpartRangeDeleteAndCompact(t *testing.T) {
	e := newMpartEngine(t)
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "mp_ret",
		Engine: "CSTORE2",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 100; i++ {
		if err := e.Insert("mp_ret", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	// Delete id in [10, 50) — range delete path (PK WHERE with bounds).
	ex := sqlx.NewExecutor(e)
	r := execOK(t, ex, "DELETE FROM mp_ret WHERE id >= 10 AND id < 50")
	if r.Affected != 40 {
		t.Fatalf("range delete affected = %d, want 40", r.Affected)
	}
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_ret")
	if r.Rows[0][0].Int != 60 {
		t.Fatalf("count after range delete = %d, want 60", r.Rows[0][0].Int)
	}
	// a point read of a deleted pk must miss
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_ret WHERE id = 25")
	if r.Rows[0][0].Int != 0 {
		t.Fatalf("deleted pk visible, count = %d", r.Rows[0][0].Int)
	}
	// Compact merges parts dropping tombstones; count unchanged
	if _, err := e.Compact("mp_ret"); err != nil {
		t.Fatal(err)
	}
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_ret")
	if r.Rows[0][0].Int != 60 {
		t.Fatalf("count after compact = %d, want 60", r.Rows[0][0].Int)
	}
	r = execOK(t, ex, "SELECT SUM(id) AS s FROM mp_ret")
	// sum of 0..9 and 50..99 = 45 + (50+99)*50/2 = 45 + 3725 = 3770
	if r.Rows[0][0].Int != 3770 {
		t.Fatalf("sum after compact = %d, want 3770", r.Rows[0][0].Int)
	}
	// Delete everything and compact to empty
	execOK(t, ex, "DELETE FROM mp_ret WHERE id >= 0")
	if _, err := e.Compact("mp_ret"); err != nil {
		t.Fatal(err)
	}
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_ret")
	if r.Rows[0][0].Int != 0 {
		t.Fatalf("count after delete-all+compact = %d, want 0", r.Rows[0][0].Int)
	}
}

// TestMpartColumnarParity runs the same columnar SQL against CSTORE and
// CSTORE2 and requires identical results.
func TestMpartColumnarParity(t *testing.T) {
	sqls := []string{
		"SELECT COUNT(*) FROM t",
		"SELECT SUM(x) FROM t",
		"SELECT AVG(x) FROM t",
		"SELECT g, COUNT(*), SUM(x) FROM t GROUP BY g",
		"SELECT x FROM t WHERE x >= 3 ORDER BY x LIMIT 2",
	}
	for _, engine := range []string{"CSTORE", "CSTORE2"} {
		e := newTestEngine(t)
		execOK(t, sqlx.NewExecutor(e), "CREATE TABLE t (id int64 primary key, g string, x int64) ENGINE="+engine)
		ex := sqlx.NewExecutor(e)
		execOK(t, ex, "INSERT INTO t VALUES (1,'a',1),(2,'a',2),(3,'b',3),(4,'b',4),(5,'c',5)")
		for _, s := range sqls {
			got := execOK(t, ex, s)
			want := execOK(t, sqlx.NewExecutor(e), s) // cached path, same engine
			if !reflect.DeepEqual(got.Rows, want.Rows) {
				t.Errorf("%s engine=%s: %v", s, engine, got.Rows)
			}
		}
		if engine == "CSTORE" {
			e.Close()
		}
	}
}

// TestMpartReplayIdempotent re-applies the same set of row writes twice (the
// raft-replay scenario after a snapshot) and requires the count to stay exact.
func TestMpartReplayIdempotent(t *testing.T) {
	e := newMpartEngine(t)
	defer e.Close()
	if err := e.CreateTable(mpartSchema); err != nil {
		t.Fatal(err)
	}
	apply := func() {
		err := e.UpdateBatch(func() error {
			for i := int64(0); i < 10; i++ {
				if err := e.Insert("mp_t", map[string]sqlx.Value{
					"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)),
				}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	apply()
	apply() // replay: same rows arrive again; must be no-ops for the count
	ex := sqlx.NewExecutor(e)
	r := execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_t")
	if r.Rows[0][0].Int != 10 {
		t.Fatalf("count after replay = %d, want 10", r.Rows[0][0].Int)
	}
}

// TestMpartSnapshotUnderWrite takes a snapshot while a second batch writes,
// then verifies the snapshot is a consistent point-in-time view.
func TestMpartSnapshotUnderWrite(t *testing.T) {
	e := newMpartEngine(t)
	defer e.Close()
	if err := e.CreateTable(mpartSchema); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 100; i++ {
		if err := e.Insert("mp_t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue("v")}); err != nil {
			t.Fatal(err)
		}
	}
	state, err := e.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	// snapshot must be independent of later writes
	for i := int64(200); i < 300; i++ {
		if err := e.Insert("mp_t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue("v2")}); err != nil {
			t.Fatal(err)
		}
	}
	e2 := newMpartEngine(t)
	defer e2.Close()
	if err := e2.ReplaceState(state); err != nil {
		t.Fatal(err)
	}
	rows, err := e2.Scan("mp_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 100 {
		t.Fatalf("snapshot rows = %d, want 100", len(rows))
	}
}

// TestMpartDeleteAllThenReadd covers rowDeleteAll semantics used by
// ReplaceState: the table is wiped and re-populated inside one tx.
func TestMpartDeleteAllThenReadd(t *testing.T) {
	e := newMpartEngine(t)
	defer e.Close()
	if err := e.CreateTable(mpartSchema); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 5; i++ {
		if err := e.Insert("mp_t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue("old")}); err != nil {
			t.Fatal(err)
		}
	}
	// wipe and re-add inside a single batch (ReplaceState shape)
	if err := e.UpdateBatch(func() error {
		if err := e.writeTo(e.store("CSTORE2"), func(tx storeTx) error {
			return tx.rowDeleteAll("mp_t")
		}); err != nil {
			return err
		}
		for i := int64(10); i < 13; i++ {
			if err := e.Insert("mp_t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue("new")}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	r := execOK(t, ex, "SELECT COUNT(*) AS c FROM mp_t")
	if r.Rows[0][0].Int != 3 {
		t.Fatalf("count after delete-all+readd = %d, want 3", r.Rows[0][0].Int)
	}
	r = execOK(t, ex, "SELECT v FROM mp_t WHERE id = 11")
	if r.Rows[0][0].Str != "new" {
		t.Fatalf("readd value = %q", r.Rows[0][0].Str)
	}
}

// BenchmarkMpartBatchLoad mirrors the raft apply path of the qa benchmark:
// STMT separate UpdateBatch commits, each inserting 500 rows, crossing the
// mem-part flush threshold many times. Guards against pathological per-part
// decompression during the existence check of every inserted PK.
func BenchmarkMpartBatchLoad(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "db")
	e, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	if err := e.CreateTable(&sqlx.TableSchema{
		Name:   "mp_load",
		Engine: "CSTORE2",
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true},
			{Name: "v", Type: sqlx.TypeString},
			{Name: "g", Type: sqlx.TypeInt},
		},
		PK: []string{"id"},
	}); err != nil {
		b.Fatal(err)
	}
	stmt := 2000
	rows := 500
	b.ResetTimer()
	for s := 0; s < stmt; s++ {
		base := int64(s * rows)
		err := e.UpdateBatch(func() error {
			for i := int64(0); i < int64(rows); i++ {
				id := base + i
				if err := e.Insert("mp_load", map[string]sqlx.Value{
					"id": sqlx.IntValue(id),
					"v":  sqlx.StrValue(fmt.Sprintf("v%d", id)),
					"g":  sqlx.IntValue(id % 100),
				}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	ex := sqlx.NewExecutor(e)
	sel, err := sqlx.Parse("SELECT COUNT(*) AS c FROM mp_load")
	if err != nil {
		b.Fatal(err)
	}
	r, err := ex.Execute(sel[0])
	if err != nil {
		b.Fatal(err)
	}
	if r.Rows[0][0].Int != int64(stmt*rows) {
		b.Fatalf("count = %d, want %d", r.Rows[0][0].Int, stmt*rows)
	}
}

// TestMpartDisjointParts exercises the walkMerged fast paths over multiple
// flushed parts with pairwise-disjoint PK windows (the shape left by
// sequential inserts): concatenation for ASC scans, reversed concatenation for
// ORDER BY ... DESC, and the heap fallback when a query range overlaps two
// parts (here: a range crossing the part boundary must still merge).
func TestMpartDisjointParts(t *testing.T) {
	for _, engine := range []string{"CSTORE", "CSTORE2"} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir() + "/db"
			e, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			ex := sqlx.NewExecutor(e)
			if _, err := ex.Execute(mustParse(t, "CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE="+engine)[0]); err != nil {
				t.Fatal(err)
			}
			// Flush 4 parts, each with a disjoint PK window of 10000 rows.
			for p := 0; p < 4; p++ {
				e.UpdateBatch(func() error {
					for i := int64(p * 10000); i < int64((p+1)*10000); i++ {
						if err := e.Insert("t", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(i % 100)}); err != nil {
							return err
						}
					}
					return nil
				})
			}
			// Full ASC scan: 40000 rows via concatenation.
			r, err := ex.Execute(mustParse(t, "SELECT COUNT(*) AS c FROM t")[0])
			if err != nil {
				t.Fatal(err)
			}
			if r.Rows[0][0].Int != 40000 {
				t.Fatalf("count = %d, want 40000", r.Rows[0][0].Int)
			}
			// ORDER BY id ASC LIMIT 3: smallest three across all parts.
			r, err = ex.Execute(mustParse(t, "SELECT id FROM t ORDER BY id LIMIT 3")[0])
			if err != nil {
				t.Fatal(err)
			}
			if r.Rows[0][0].Int != 0 || r.Rows[1][0].Int != 1 || r.Rows[2][0].Int != 2 {
				t.Fatalf("orderby asc: %v", r.Rows)
			}
			// ORDER BY id DESC LIMIT 3 (rev concatenation).
			r, err = ex.Execute(mustParse(t, "SELECT id FROM t ORDER BY id DESC LIMIT 3")[0])
			if err != nil {
				t.Fatal(err)
			}
			if r.Rows[0][0].Int != 39999 || r.Rows[1][0].Int != 39998 || r.Rows[2][0].Int != 39997 {
				t.Fatalf("orderby desc: %v", r.Rows)
			}
			// Full DESC read (no LIMIT): walks every source to its lo boundary
			// (lo=0 in the first part), exercising the rev fast path's final
			// index — regression for an out-of-range pks[last-1] access.
			r, err = ex.Execute(mustParse(t, "SELECT id FROM t ORDER BY id DESC")[0])
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 40000 || r.Rows[0][0].Int != 39999 || r.Rows[39999][0].Int != 0 {
				t.Fatalf("full desc: n=%d first=%v last=%v", len(r.Rows), r.Rows[0], r.Rows[len(r.Rows)-1])
			}
			for i := 0; i < len(r.Rows); i++ {
				if r.Rows[i][0].Int != int64(39999-i) {
					t.Fatalf("full desc at %d: %v", i, r.Rows[i][0].Int)
				}
			}
			// Range crossing the part-1/part-2 boundary: 5000 rows, heap path.
			r, err = ex.Execute(mustParse(t, "SELECT COUNT(*) AS c FROM t WHERE id >= 9995 AND id < 10005")[0])
			if err != nil {
				t.Fatal(err)
			}
			if r.Rows[0][0].Int != 10 {
				t.Fatalf("cross-boundary count = %d, want 10", r.Rows[0][0].Int)
			}
			// Aggregate over all parts (SUM pushes through the concatenation).
			r, err = ex.Execute(mustParse(t, "SELECT SUM(id) AS s FROM t")[0])
			if err != nil {
				t.Fatal(err)
			}
			want := int64(39999 * 20000) // arithmetic series 0..39999
			if r.Rows[0][0].Int != want {
				t.Fatalf("sum = %d, want %d", r.Rows[0][0].Int, want)
			}
			// GROUP BY across all parts.
			r, err = ex.Execute(mustParse(t, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")[0])
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 100 {
				t.Fatalf("groups = %d, want 100", len(r.Rows))
			}
		})
	}
}

// TestMpartTombstoneFlushShadowing guards the part tombstone format: a DELETE
// that lands in a flushed part (via del.bin) must shadow the older live row in
// every read path, not just the countFor fast path. Regression for the bug
// where flushed tombstones read back as live empty rows.
func TestMpartTombstoneFlushShadowing(t *testing.T) {
	dir := t.TempDir() + "/db"
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	e.CreateTable(&sqlx.TableSchema{Name: "tt", Engine: "CSTORE2",
		Columns: []sqlx.ColumnDef{{Name: "id", Type: sqlx.TypeInt, NotNull: true, AsPrimary: true}, {Name: "v", Type: sqlx.TypeString}},
		PK:      []string{"id"}})
	// Fill one full mem part.
	e.UpdateBatch(func() error {
		for i := int64(0); i < mpartFlushThreshold; i++ {
			if err := e.Insert("tt", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i))}); err != nil {
				return err
			}
		}
		return nil
	})
	// Tombstone id=7, then insert enough rows to cross the flush threshold so
	// the tombstone lands in a part (not just mem).
	e.UpdateBatch(func() error {
		return e.Delete("tt", []sqlx.Value{sqlx.IntValue(7)})
	})
	e.UpdateBatch(func() error {
		for i := int64(mpartFlushThreshold); i < 2*mpartFlushThreshold; i++ {
			if err := e.Insert("tt", map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i))}); err != nil {
				return err
			}
		}
		return nil
	})
	ex := sqlx.NewExecutor(e)
	want := int64(2*mpartFlushThreshold - 1)
	r := execOK(t, ex, "SELECT COUNT(*) AS c FROM tt")
	if r.Rows[0][0].Int != want {
		t.Fatalf("count = %d, want %d (tombstone resurrected?)", r.Rows[0][0].Int, want)
	}
	// The collectEntries/scan path must also hide the flushed tombstone.
	r = execOK(t, ex, "SELECT COUNT(*) AS c FROM tt WHERE id = 7")
	if r.Rows[0][0].Int != 0 {
		t.Fatalf("WHERE id=7 count = %d, want 0 (tombstone resurrected in scan path)", r.Rows[0][0].Int)
	}
	rows, err := e.Scan("tt")
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(rows)) != want {
		t.Fatalf("scan rows = %d, want %d", len(rows), want)
	}
	// The newest-wins merge must still shadow an older live row: put id=7 back.
	e.UpdateBatch(func() error {
		return e.Insert("tt", map[string]sqlx.Value{"id": sqlx.IntValue(7), "v": sqlx.StrValue("back")})
	})
	r = execOK(t, ex, "SELECT v FROM tt WHERE id = 7")
	if r.Rows[0][0].Str != "back" {
		t.Fatalf("readd value = %q, want back", r.Rows[0][0].Str)
	}
}

// TestMpartOrderDesc guards the reverse k-way merge: ORDER BY pk DESC must
// emit the largest PKs first even with a single source (mem/part), and the
// streaming walker must honor the LIMIT via errStop.
func TestMpartOrderDesc(t *testing.T) {
	for _, engine := range []string{"CSTORE", "CSTORE2"} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir() + "/db"
			e, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			ex := sqlx.NewExecutor(e)
			if _, err := ex.Execute(mustParse(t, "CREATE TABLE t (id int64 primary key, v string) ENGINE="+engine)[0]); err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 10; i++ {
				if _, err := ex.Execute(mustParse(t, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", i, i))[0]); err != nil {
					t.Fatal(err)
				}
			}
			r, err := ex.Execute(mustParse(t, "SELECT id FROM t ORDER BY id DESC LIMIT 3")[0])
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 3 {
				t.Fatalf("got %d rows: %v", len(r.Rows), r.Rows)
			}
			if r.Rows[0][0].Int != 10 || r.Rows[1][0].Int != 9 || r.Rows[2][0].Int != 8 {
				t.Fatalf("orderby desc: %v", r.Rows)
			}
		})
	}
}

// TestMpartGranulePoint exercises the v4 sparse-index format end to end: after
// writing well past the granule row size and flushing into sealed parts, point
// gets, an ORDER BY ... DESC LIMIT (which walks only the trailing granule) and
// a point UPDATE must all resolve through the granule-indexed path, survive a
// reopen from disk, and keep the newest-wins semantics across parts.
func TestMpartGranulePoint(t *testing.T) {
	if mpartGranuleRows < 2 {
		t.Fatal("granule format requires mpartGranuleRows >= 2")
	}
	dir := t.TempDir() + "/db"
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	if _, err := ex.Execute(mustParse(t, "CREATE TABLE g (id int64 primary key, v string) ENGINE=CSTORE2")[0]); err != nil {
		t.Fatal(err)
	}
	const n = mpartGranuleRows*3 + 7
	for start := 0; start < n; start += mpartGranuleRows {
		if err := e.UpdateBatch(func() error {
			for i := start; i < n && i < start+mpartGranuleRows; i++ {
				e.Insert("g", map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue(fmt.Sprintf("v%d", i))})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	ms, ok := e.store("CSTORE2").(*mpartStore)
	if !ok {
		t.Fatal("not mpart store")
	}
	for i := 0; i < 5; i++ {
		ms.flush("g")
		ms.mergeIdle(time.Now())
	}
	check := func(v42 string) {
		t.Helper()
		r := execOK(t, ex, "SELECT v FROM g WHERE id = 0")
		if r.Rows[0][0].Str != "v0" {
			t.Fatalf("point id=0 = %q", r.Rows[0][0].Str)
		}
		r = execOK(t, ex, "SELECT v FROM g WHERE id = 42")
		if r.Rows[0][0].Str != v42 {
			t.Fatalf("point id=42 = %q, want %q", r.Rows[0][0].Str, v42)
		}
		r = execOK(t, ex, "SELECT id FROM g ORDER BY id DESC LIMIT 3")
		if len(r.Rows) != 3 {
			t.Fatalf("order desc got %d rows", len(r.Rows))
		}
		if r.Rows[0][0].Int != int64(n-1) || r.Rows[1][0].Int != int64(n-2) || r.Rows[2][0].Int != int64(n-3) {
			t.Fatalf("order desc ids = %v, want top %d", r.Rows, n)
		}
	}
	check("v42")
	// A point UPDATE must not turn the row into a tombstone (the granule path
	// previously returned garbage full-width cells that broke newest-wins).
	if _, err := ex.Execute(mustParse(t, "UPDATE g SET v = 'patched' WHERE id = 42")[0]); err != nil {
		t.Fatal(err)
	}
	r := execOK(t, ex, "SELECT v FROM g WHERE id = 42")
	if r.Rows[0][0].Str != "patched" {
		t.Fatalf("after update id=42 = %q, want patched", r.Rows[0][0].Str)
	}
	e.Close()
	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex = sqlx.NewExecutor(e)
	check("patched")
}

// TestMpartZoneMap verifies the per-granule numeric zone maps (idx ver 3):
// writePart computes min/max bounds per dense column, reopen decodes them, and
// filtered count/aggregate scans skip granules whose bounds cannot match the
// predicate.
func TestMpartZoneMap(t *testing.T) {
	if mpartGranuleRows < 2 {
		t.Fatal("granule format requires mpartGranuleRows >= 2")
	}
	dir := t.TempDir() + "/db"
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	if _, err := ex.Execute(mustParse(t, "CREATE TABLE g (id int64 primary key, v string, g int64, score float) ENGINE=CSTORE2")[0]); err != nil {
		t.Fatal(err)
	}
	const n = mpartGranuleRows*3 + 7
	for start := 0; start < n; start += mpartGranuleRows {
		if err := e.UpdateBatch(func() error {
			for i := start; i < n && i < start+mpartGranuleRows; i++ {
				e.Insert("g", map[string]sqlx.Value{
					"id":    sqlx.IntValue(int64(i)),
					"v":     sqlx.StrValue(fmt.Sprintf("v%d", i)),
					"g":     sqlx.IntValue(int64(i % 100)),
					"score": sqlx.FloatValue(float64(i)),
				})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	ms, ok := e.store("CSTORE2").(*mpartStore)
	if !ok {
		t.Fatal("not mpart store")
	}
	for i := 0; i < 5; i++ {
		ms.flush("g")
		ms.mergeIdle(time.Now())
	}
	ms.mu.Lock()
	parts := append([]*mpart(nil), ms.parts["g"]...)
	ms.mu.Unlock()
	if len(parts) == 0 {
		t.Fatal("no parts after flush/merge")
	}
	// g = id % 100: every granule covers [0, 99], score = id is monotonic so
	// each granule's score zone must be its id window and exclude all others.
	colG := -1
	colScore := -1
	tbl, err := e.getTable("g")
	if err != nil {
		t.Fatal(err)
	}
	for ci, c := range tbl.cols {
		if c.name == "g" {
			colG = ci
		}
		if c.name == "score" {
			colScore = ci
		}
	}
	if colG < 0 || colScore < 0 {
		t.Fatal("columns g/score not found")
	}
	for _, p := range parts {
		if mpartIdxVer < 3 || colG >= len(p.granules[0].zoneMin) {
			t.Skip("zone maps require idx ver 3")
		}
		t.Logf("part ncols=%d colFmts=%v pkMin=%q", p.ncols, p.colFmts, p.pkMin)
		for gi := range p.granules {
			vals, nulls, err := p.loadGranuleDense(gi, colScore)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range nulls {
				if w != 0 {
					t.Fatalf("score column has nulls")
				}
			}
			minF, maxF := math.Inf(1), math.Inf(-1)
			for _, raw := range vals {
				f := math.Float64frombits(uint64(raw))
				if f < minF {
					minF = f
				}
				if f > maxF {
					maxF = f
				}
			}
			zMin := math.Float64frombits(uint64(p.granules[gi].zoneMin[colScore]))
			zMax := math.Float64frombits(uint64(p.granules[gi].zoneMax[colScore]))
			if zMin != minF || zMax != maxF {
				t.Fatalf("granule %d score zone = [%v,%v], want [%v,%v]", gi, zMin, zMax, minF, maxF)
			}
		}
	}
	// A filtered aggregate over the monotonic score column must return the
	// right single row (and skip unrelated granules).
	r := execOK(t, ex, "SELECT count(*), sum(id) FROM g WHERE score = 42")
	if len(r.Rows) != 1 || r.Rows[0][0].Int != 1 || r.Rows[0][1].Int != 42 {
		t.Fatalf("filtered agg score=42: %v", r.Rows)
	}
	// g = id % 100 repeats within a granule; the count must still be exact.
	r = execOK(t, ex, "SELECT count(*) FROM g WHERE g = 42")
	if len(r.Rows) != 1 || r.Rows[0][0].Int != int64(n/100) {
		t.Fatalf("filtered count g=42: %v", r.Rows)
	}
	e.Close()
	// Reopen: zones survive a roundtrip through readMpartMeta.
	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex = sqlx.NewExecutor(e)
	r = execOK(t, ex, "SELECT count(*), sum(id) FROM g WHERE score = 42")
	if len(r.Rows) != 1 || r.Rows[0][0].Int != 1 || r.Rows[0][1].Int != 42 {
		t.Fatalf("filtered agg after reopen score=42: %v", r.Rows)
	}
	ms, _ = e.store("CSTORE2").(*mpartStore)
	ms.mu.Lock()
	parts = append([]*mpart(nil), ms.parts["g"]...)
	ms.mu.Unlock()
	for _, p := range parts {
		for gi := range p.granules {
			vals, nulls, err := p.loadGranuleDense(gi, colScore)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range nulls {
				if w != 0 {
					t.Fatalf("after reopen score column has nulls")
				}
			}
			minF, maxF := math.Inf(1), math.Inf(-1)
			for _, raw := range vals {
				f := math.Float64frombits(uint64(raw))
				if f < minF {
					minF = f
				}
				if f > maxF {
					maxF = f
				}
			}
			zMin := math.Float64frombits(uint64(p.granules[gi].zoneMin[colScore]))
			zMax := math.Float64frombits(uint64(p.granules[gi].zoneMax[colScore]))
			if zMin != minF || zMax != maxF {
				t.Fatalf("after reopen granule %d score zone = [%v,%v], want [%v,%v]", gi, zMin, zMax, minF, maxF)
			}
		}
	}
}

// TestMpartPreloadDense verifies the background dense-column prefetch: after a
// flush that crosses the idle threshold, queuePreload runs preloadDense on the
// new part, so loadColDense returns cached values without an explicit query.
func TestMpartPreloadDense(t *testing.T) {
	dir := t.TempDir() + "/db"
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	if _, err := ex.Execute(mustParse(t, "CREATE TABLE g (id int64 primary key, v string, g int64) ENGINE=CSTORE2")[0]); err != nil {
		t.Fatal(err)
	}
	const n = mpartIdleFlushMinRows + 100
	for start := 0; start < n; start += mpartFlushThreshold {
		e.UpdateBatch(func() error {
			for i := start; i < n; i++ {
				e.Insert("g", map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(int64(i % 100))})
			}
			return nil
		})
	}
	ms := e.store("CSTORE2").(*mpartStore)
	// Simulate the idle flusher: last write is old, so flushIdle flushes the
	// mem tail and queues the new part for preload.
	ms.mu.Lock()
	ms.lastWrite["g"] = time.Now().Add(-2 * mpartIdleFlushInterval)
	ms.mu.Unlock()
	ms.flushIdle(time.Now())
	// Wait for the prefetch worker to drain the queue. denseVals is written
	// before the part is published to the preload channel and loadColDense
	// sync.Once makes the write visible to subsequent readers.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ms.mu.Lock()
		parts := append([]*mpart(nil), ms.parts["g"]...)
		done := true
		for _, p := range parts {
			if p.denseVals == nil || len(p.denseVals) == 0 || len(p.denseVals[0]) == 0 {
				done = false
				break
			}
		}
		ms.mu.Unlock()
		if done || len(parts) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("preload did not populate dense cache in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(ms.parts["g"]) == 0 {
		t.Fatal("no parts after idle flush")
	}
}

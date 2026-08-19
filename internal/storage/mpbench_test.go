package storage

import (
	"fmt"
	"testing"

	sqlx "ydbgo/internal/sql"
)

// BenchmarkSum15Parts replicates the cluster part shape: 15 parts of ~66k rows
// (1M rows flushed in ~66k-row parts).
func BenchmarkSum15Parts(b *testing.B) {
	for _, engine := range []string{"CSTORE2", "CSTORE"} {
		e, err := Open(b.TempDir() + "/db")
		if err != nil {
			b.Fatal(err)
		}
		ex := sqlx.NewExecutor(e)
		tn := "bt"
		if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, v string, g int64) ENGINE="+engine)[0]); err != nil {
			b.Fatal(err)
		}
		// 15 parts x 66667 rows, each part flushed by crossing the threshold.
		n := 0
		for int64(n) < 1000000 {
			start := int64(n)
			e.UpdateBatch(func() error {
				for i := start; i < start+66667 && i < 1000000; i++ {
					if err := e.Insert(tn, map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(i % 100)}); err != nil {
						return err
					}
				}
				return nil
			})
			n += 66667
		}
		st := mustParse(b, "SELECT SUM(id) AS s FROM "+tn)[0]
		b.Run(engine+"/sum_15parts", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ex.Execute(st); err != nil {
					b.Fatal(err)
				}
			}
		})
		e.Close()
	}
}

// BenchmarkGroupSinglePart measures GROUP BY on one 1M-row part (the post-merge
// cluster shape): the dense column decodes and the group map are the whole
// cost, so this is the floor the cluster execshard path adds RPC on top of.
func BenchmarkGroupSinglePart(b *testing.B) {
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	tn := "bt"
	if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, v string, g int64) ENGINE=CSTORE2")[0]); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < 1000000; start += 65536 {
		end := start + 65536
		if end > 1000000 {
			end = 1000000
		}
		s := start
		e.UpdateBatch(func() error {
			for i := s; i < end; i++ {
				if err := e.Insert(tn, map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(int64(i % 100))}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if _, err := e.Compact(tn); err != nil {
		b.Fatal(err)
	}
	st := mustParse(b, "SELECT g, COUNT(*) AS c, SUM(id) AS s FROM "+tn+" GROUP BY g")[0]
	for i := 0; i < 3; i++ {
		if _, err := ex.Execute(st); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("group_single_part_warm", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := ex.Execute(st); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestGroupAlloc isolates the per-op allocation of the GROUP BY aggregation
// loop on the merged single-part shape, independent of Go's benchmark harness.
func TestGroupAlloc(t *testing.T) {
	e, err := Open(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	tn := "bt"
	if _, err := ex.Execute(mustParse(t, "CREATE TABLE "+tn+" (id int64 primary key, v string, g int64) ENGINE=CSTORE2")[0]); err != nil {
		t.Fatal(err)
	}
	for start := 0; start < 1000000; start += 65536 {
		end := start + 65536
		if end > 1000000 {
			end = 1000000
		}
		s := start
		e.UpdateBatch(func() error {
			for i := s; i < end; i++ {
				if err := e.Insert(tn, map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(int64(i % 100))}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if _, err := e.Compact(tn); err != nil {
		t.Fatal(err)
	}
	st := mustParse(t, "SELECT g, COUNT(*) AS c, SUM(id) AS s FROM "+tn+" GROUP BY g")[0]
	if _, err := ex.Execute(st); err != nil {
		t.Fatal(err)
	}
	// The grouped aggregate path allocates a scratch numVec per group; this
	// guards against a regression to quadratic scratch growth (alloc ~24MB/op).
	r := execOK(t, ex, "SELECT COUNT(*) AS c FROM "+tn)
	if r.Rows[0][0].Int != 1000000 {
		t.Fatalf("count = %d, want 1000000", r.Rows[0][0].Int)
	}
}

// BenchmarkFilteredSumNoPrune measures a WHERE on a column whose value repeats
// inside every granule (g = id % 100, so each granule's zone covers [0,99] and
// nothing can be pruned): the zone-map worst case.
func BenchmarkFilteredSumNoPrune(b *testing.B) {
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	tn := "bt"
	if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, v string, g int64) ENGINE=CSTORE2")[0]); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < 1000000; start += 65536 {
		end := start + 65536
		if end > 1000000 {
			end = 1000000
		}
		s := start
		e.UpdateBatch(func() error {
			for i := s; i < end; i++ {
				if err := e.Insert(tn, map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(int64(i % 100))}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if _, err := e.Compact(tn); err != nil {
		b.Fatal(err)
	}
	st := mustParse(b, "SELECT SUM(g) AS s FROM "+tn+" WHERE g = 42")[0]
	b.Run("filtered_sum_g_eq_42", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := ex.Execute(st); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFilteredSumPrune measures a WHERE on a monotonically increasing
// column (score = id): each granule's zone excludes every other granule's
// range, so the filtered scan decodes exactly one granule instead of the whole
// 1M-row part. This is the zone-map payoff path.
func BenchmarkFilteredSumPrune(b *testing.B) {
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	ex := sqlx.NewExecutor(e)
	tn := "bt"
	if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, v string, score int64) ENGINE=CSTORE2")[0]); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < 1000000; start += 65536 {
		end := start + 65536
		if end > 1000000 {
			end = 1000000
		}
		s := start
		e.UpdateBatch(func() error {
			for i := s; i < end; i++ {
				if err := e.Insert(tn, map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "score": sqlx.IntValue(int64(i))}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if _, err := e.Compact(tn); err != nil {
		b.Fatal(err)
	}
	st := mustParse(b, "SELECT SUM(score) AS s FROM "+tn+" WHERE score = 42")[0]
	b.Run("filtered_sum_score_eq_42", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := ex.Execute(st); err != nil {
				b.Fatal(err)
			}
		}
	})
}

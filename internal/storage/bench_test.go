package storage

import (
	"fmt"
	"testing"

	sqlx "ydbgo/internal/sql"
)

const benchRows = 10000

// benchOLAPEngine builds an engine with a loaded table of benchRows and
// returns it with an executor. engine is "" (TABLE) or "CSTORE".
func benchOLAPEngine(b testing.TB, engine string) (*Engine, *sqlx.Executor, string) {
	b.Helper()
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	name := "TABLE"
	engineClause := ""
	if engine != "" {
		name = engine
		engineClause = " ENGINE=" + engine
	}
	tn := "bt_" + name
	if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, cat string, score int64, flag bool)"+engineClause)[0]); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < benchRows; i += 1000 {
		s := "INSERT INTO " + tn + " VALUES "
		for j := i; j < i+1000 && j < benchRows; j++ {
			if j > i {
				s += ", "
			}
			s += fmt.Sprintf("(%d, 'cat%d', %d, %t)", j, j%10, j*3, j%2 == 0)
		}
		if _, err := ex.Execute(mustParse(b, s)[0]); err != nil {
			b.Fatal(err)
		}
	}
	return e, ex, tn
}

// BenchmarkOLAP compares the row store (TABLE) against the columnar store
// (CSTORE) on OLAP-shaped SELECTs: full scan, column projection and whole
// table aggregates (the CSTORE columnar/aggregate pushdown path).
func BenchmarkOLAP(b *testing.B) {
	queries := []struct {
		name string
		sql  string
	}{
		{"scan_all", "SELECT * FROM %s"},
		{"projection", "SELECT cat, score FROM %s"},
		{"aggregate", "SELECT COUNT(*), SUM(score), MAX(score), MIN(score), AVG(score) FROM %s"},
		{"aggregate_where", "SELECT SUM(score) FROM %s WHERE id > 5000"},
		{"count_star", "SELECT COUNT(*) FROM %s"},
		{"sum_one", "SELECT SUM(score) FROM %s"},
		{"count_range", "SELECT COUNT(*) FROM %s WHERE id >= 2000 AND id < 5000"},
		{"sum_range", "SELECT SUM(score) FROM %s WHERE id >= 2000 AND id < 5000"},
		{"aggregate_window", "SELECT SUM(score), MAX(score) FROM %s WHERE id >= 2000 AND id < 5000"},
		{"projection_range", "SELECT cat, score FROM %s WHERE id >= 2000 AND id < 5000"},
		{"groupby", "SELECT cat, COUNT(*), SUM(score) FROM %s GROUP BY cat"},
		{"groupby_range", "SELECT cat, COUNT(*) FROM %s WHERE id >= 2000 AND id < 5000 GROUP BY cat"},
	}
	for _, engine := range []string{"", "CSTORE", "CSTORE2"} {
		label := "TABLE"
		if engine != "" {
			label = engine
		}
		e, ex, tn := benchOLAPEngine(b, engine)
		for _, q := range queries {
			st := mustParse(b, fmt.Sprintf(q.sql, tn))[0]
			b.Run(label+"/"+q.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := ex.Execute(st); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		e.Close()
	}
}

// BenchmarkIndex compares the filtered columnar scan against the secondary
// index fast path for equality and leading-literal LIKE predicates on a
// CSTORE table (idx / scan sub-benchmarks).
func BenchmarkIndex(b *testing.B) {
	queries := []struct {
		name string
		sql  string
	}{
		{"eq_rare", "SELECT COUNT(*) FROM %s WHERE cat = 'cat3'"},
		{"eq_dense", "SELECT COUNT(*) FROM %s WHERE cat = 'cat0'"},
		{"like", "SELECT COUNT(*) FROM %s WHERE cat LIKE 'cat1%%'"},
	}
	for _, q := range queries {
		for _, indexed := range []bool{false, true} {
			e, ex, tn := benchOLAPEngine(b, "CSTORE")
			label := "scan"
			if indexed {
				label = "index"
				if _, err := ex.Execute(mustParse(b, "CREATE INDEX ix_cat ON "+tn+" (cat)")[0]); err != nil {
					b.Fatal(err)
				}
			}
			st := mustParse(b, fmt.Sprintf(q.sql, tn))[0]
			b.Run(label+"/"+q.name, func(b *testing.B) {
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
}

// BenchmarkBatchInsert compares inserting N rows one Put at a time against the
// single-transaction BatchInsert API (same engine, isolated from raft).
func BenchmarkBatchInsert(b *testing.B) {
	for _, batch := range []int{1, 10, 100, 1000} {
		e, err := Open(b.TempDir() + "/db")
		if err != nil {
			b.Fatal(err)
		}
		if err := e.CreateTable(&sqlx.TableSchema{
			Name:    "t",
			Columns: []sqlx.ColumnDef{{Name: "id", Type: sqlx.TypeInt}, {Name: "v", Type: sqlx.TypeString}},
			PK:      []string{"id"},
			Engine:  "CSTORE",
		}); err != nil {
			b.Fatal(err)
		}
		rows := make([]map[string]sqlx.Value, batch)
		for i := range rows {
			rows[i] = map[string]sqlx.Value{"id": sqlx.IntValue(int64(i)), "v": sqlx.StrValue("value")}
		}
		b.Run(fmt.Sprintf("batch%d", batch), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.BatchInsert("t", rows); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("perrow%d", batch), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range rows {
					if err := e.Insert("t", r); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		e.Close()
	}
}

// BenchmarkCStoreGroupedDirect measures just the columnar grouped-aggregate
// engine call (no SQL parsing/planning overhead).
func BenchmarkCStoreGroupedDirect(b *testing.B) {
	e, ex, tn := benchOLAPEngine(b, "CSTORE")
	defer e.Close()
	_ = ex
	tn = "bt_cstore"
	gi := -1
	for i, c := range e.mustSchema(b, tn).Columns {
		if c.Name == "cat" {
			gi = i
		}
	}
	if gi < 0 {
		b.Fatal("no cat column")
	}
	sc := -1
	for i, c := range e.mustSchema(b, tn).Columns {
		if c.Name == "score" {
			sc = i
		}
	}
	gas := []sqlx.GroupAgg{{Col: -1, Aggs: []string{"count"}}, {Col: sc, Aggs: []string{"sum"}}, {Col: sc, Aggs: []string{"count"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.ColumnGroupedAggregates(tn, gi, gas, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func (e *Engine) mustSchema(b testing.TB, name string) *sqlx.TableSchema {
	b.Helper()
	s, err := e.GetSchema(name)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// BenchmarkRead1M compares CSTORE against CSTORE2 on OLAP-shaped reads over a
// 1M-row table that has been flushed to multiple parts. The read-path
// optimizations (k-way merge, memoized per-part columns, point-PK fast path)
// target exactly these queries.
func BenchmarkRead1M(b *testing.B) {
	queries := []struct {
		name, sql string
	}{
		{"sum", "SELECT SUM(id) FROM %s"},
		{"count", "SELECT COUNT(*) FROM %s"},
		{"groupby", "SELECT g, COUNT(*) FROM %s GROUP BY g"},
		{"orderby_desc_limit", "SELECT id FROM %s ORDER BY id DESC LIMIT 10"},
		{"point", "SELECT v FROM %s WHERE id = 42"},
	}
	for _, engine := range []string{"CSTORE", "CSTORE2"} {
		e, ex, tn := bench1MEngine(b, engine)
		for _, q := range queries {
			st := mustParse(b, fmt.Sprintf(q.sql, tn))[0]
			b.Run(engine+"/"+q.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := ex.Execute(st); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		e.Close()
	}
}

func bench1MEngine(b *testing.B, engine string) (*Engine, *sqlx.Executor, string) {
	b.Helper()
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	tn := "bt"
	if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, v string, g int64) ENGINE="+engine)[0]); err != nil {
		b.Fatal(err)
	}
	e.UpdateBatch(func() error {
		for i := int64(0); i < 1000000; i++ {
			if err := e.Insert(tn, map[string]sqlx.Value{"id": sqlx.IntValue(i), "v": sqlx.StrValue(fmt.Sprintf("v%d", i)), "g": sqlx.IntValue(i % 100)}); err != nil {
				return err
			}
		}
		return nil
	})
	return e, ex, tn
}

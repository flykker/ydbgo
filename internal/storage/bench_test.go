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
	for _, engine := range []string{"", "CSTORE"} {
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

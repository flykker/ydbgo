package storage

import (
	"testing"

	sqlx "ydbgo/internal/sql"
)

// benchBigCStore builds a CSTORE engine with rows rows (default 192000, the
// README workload size) and returns it plus a raw ColumnEngine handle.
func benchBigCStore(b testing.TB, rows int) (*Engine, string) {
	b.Helper()
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	ex := sqlx.NewExecutor(e)
	tn := "big"
	if _, err := ex.Execute(mustParse(b, "CREATE TABLE "+tn+" (id int64 primary key, cat string, score int64, lat double, flag bool) ENGINE=CSTORE")[0]); err != nil {
		b.Fatal(err)
	}
	const chunk = 1000
	for i := 0; i < rows; i += chunk {
		s := "INSERT INTO " + tn + " VALUES "
		for j := i; j < i+chunk && j < rows; j++ {
			if j > i {
				s += ", "
			}
			s += "(" + itoa(j) + ", 'cat" + itoa(j%10) + "', " + itoa(j*3) + ", 0.5, true)"
		}
		if _, err := ex.Execute(mustParse(b, s)[0]); err != nil {
			b.Fatal(err)
		}
	}
	_ = ex
	return e, tn
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	p := len(buf)
	for n > 0 {
		p--
		buf[p] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[p:])
}

func BenchmarkVecCountStar(b *testing.B) {
	e, tn := benchBigCStore(b, 192000)
	defer e.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.ColumnCount(tn, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVecAggInt(b *testing.B) {
	e, tn := benchBigCStore(b, 192000)
	defer e.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.ColumnAggregates(tn, 2, []string{"sum", "count"}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVecAggFloat(b *testing.B) {
	e, tn := benchBigCStore(b, 192000)
	defer e.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.ColumnAggregates(tn, 3, []string{"sum", "avg"}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVecGrouped(b *testing.B) {
	e, tn := benchBigCStore(b, 192000)
	defer e.Close()
	gi, sc := 1, 2
	gas := []sqlx.GroupAgg{{Col: -1, Aggs: []string{"count"}}, {Col: sc, Aggs: []string{"sum"}}, {Col: sc, Aggs: []string{"count"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.ColumnGroupedAggregates(tn, gi, gas, nil); err != nil {
			b.Fatal(err)
		}
	}
}
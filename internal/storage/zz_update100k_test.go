package storage

import (
	"testing"

	sqlx "ydbgo/internal/sql"
)

func newUpdateFixture(b *testing.B) *Engine {
	e, err := Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close() })
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "id", Type: sqlx.TypeInt}, {Name: "v", Type: sqlx.TypeString}, {Name: "g", Type: sqlx.TypeInt}},
		PK:      []string{"id"},
		Engine:  "CSTORE2",
	}
	if err := e.CreateTable(schema); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < 100000; start += 8192 {
		end := start + 8192
		rows := make([]map[string]sqlx.Value, 0, 8192)
		for i := start; i < end; i++ {
			rows = append(rows, map[string]sqlx.Value{
				"id": sqlx.IntValue(int64(i)),
				"v":  sqlx.StrValue("v"),
				"g":  sqlx.IntValue(int64(i)),
			})
		}
		if _, err := e.BatchInsert("t", rows); err != nil {
			b.Fatal(err)
		}
	}
	return e
}

func BenchmarkUpdate100kReadOnly(b *testing.B) {
	e := newUpdateFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.ScanColumns("t", []int{0, 1, 2}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate100kWriteOnly(b *testing.B) {
	e := newUpdateFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]map[string]sqlx.Value, 0, 100000)
		for i := 0; i < 100000; i++ {
			out = append(out, map[string]sqlx.Value{
				"id": sqlx.IntValue(int64(i)),
				"v":  sqlx.StrValue("v"),
				"g":  sqlx.IntValue(7),
			})
		}
		if _, err := e.BatchInsert("t", out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate100kWriteRows(b *testing.B) {
	e := newUpdateFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]sqlx.Row, 0, 100000)
		for i := 0; i < 100000; i++ {
			out = append(out, sqlx.Row{
				sqlx.IntValue(int64(i)),
				sqlx.StrValue("v"),
				sqlx.IntValue(7),
			})
		}
		if _, err := e.BatchInsertRows("t", out); err != nil {
			b.Fatal(err)
		}
	}
}

package storage

import (
	"testing"

	sqlx "ydbgo/internal/sql"
)

func BenchmarkUpdate100kWriteNoFlush(b *testing.B) {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write 50k rows: below the 65536 flush threshold, so no writePart.
		out := make([]map[string]sqlx.Value, 0, 50000)
		for i := 0; i < 50000; i++ {
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

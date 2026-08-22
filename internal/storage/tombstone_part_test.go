package storage

import (
	"testing"

	sqlx "ydbgo/internal/sql"
)

// TestScanColumnsTombstoneOnlyPart guards against a panic in walkMergedLocked
// when a columnar scan crosses a tombstone-only part (ncols=0, produced when a
// range DELETE is flushed inside a raft batch, which skips the automatic
// Compact). The part carries PKs + tombstone bits but no column data, so
// loadCol returned nil cells while walkMergedLocked indexed cells[i].
func TestScanColumnsTombstoneOnlyPart(t *testing.T) {
	e := newTestEngine(t)
	defer e.Close()
	schema := &sqlx.TableSchema{
		Name:    "t",
		Columns: []sqlx.ColumnDef{{Name: "id", Type: sqlx.TypeInt}, {Name: "v", Type: sqlx.TypeString}, {Name: "g", Type: sqlx.TypeInt}},
		PK:      []string{"id"},
		Engine:  "CSTORE2",
	}
	if err := e.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	for start := 0; start < 65536; start += 8192 {
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
			t.Fatal(err)
		}
	}
	mp, ok := e.store("CSTORE2").(*mpartStore)
	if !ok {
		t.Fatal("not mpartStore")
	}
	if err := mp.flush("t"); err != nil {
		t.Fatal(err)
	}
	// Delete inside a batch (the raft shape: DeleteRange skips the automatic
	// Compact while txActive), so the mem part becomes tombstone-only.
	err := e.UpdateBatch(func() error {
		_, derr := e.DeleteRange("t", &sqlx.PKRange{
			Lower: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(0)}, Incl: true},
			Upper: &sqlx.PKBound{Prefix: []sqlx.Value{sqlx.IntValue(4096)}, Incl: false},
		})
		return derr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mp.flush("t"); err != nil {
		t.Fatal(err)
	}
	rows, err := e.ScanColumns("t", []int{0, 1, 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 65536-4096 {
		t.Fatalf("rows=%d want %d", len(rows), 65536-4096)
	}
}

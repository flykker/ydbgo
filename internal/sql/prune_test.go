package sql

import (
	"testing"
	"time"
)

func mustTs(s string) Value {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return TimestampValue(t)
}

func pruneSchema(pk ...string) *TableSchema {
	cols := []ColumnDef{
		{Name: "ts", Type: TypeTimestamp, AsPrimary: true},
		{Name: "id", Type: TypeInt, AsPrimary: true},
		{Name: "level", Type: TypeString},
		{Name: "score", Type: TypeInt},
	}
	return &TableSchema{Name: "logs", Columns: cols, PK: pk, Engine: "CSTORE"}
}

func selWhere(t *testing.T, sql string) Expr {
	t.Helper()
	st := parseOne(t, sql)
	sel, ok := st.(*SelectStmt)
	if !ok {
		t.Fatalf("got %T", st)
	}
	return sel.Where
}

func TestPKRangeFromWhere(t *testing.T) {
	tests := []struct {
		name    string
		pk      []string
		where   string
		wantRng bool
		wantExact bool
		wantLo  *PKBound // expected lower bound (or nil)
		wantUp  *PKBound // expected upper bound (or nil)
	}{
		{
			name: "ts lower bound", pk: []string{"ts"},
			where: "WHERE ts >= '2024-01-01T00:00:00Z'",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
		},
		{
			name: "ts window", pk: []string{"ts"},
			where: "WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-02-01T00:00:00Z'",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
			wantUp: &PKBound{Prefix: []Value{mustTs("2024-02-01T00:00:00Z")}, Incl: false},
		},
		{
			name: "point on full pk", pk: []string{"ts", "id"},
			where: "WHERE ts = '2024-01-01T00:00:00Z' AND id = 42",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z"), IntValue(42)}, Incl: true},
			wantUp: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z"), IntValue(42)}, Incl: true},
		},
		{
			name: "prefix equality", pk: []string{"ts", "id"},
			where: "WHERE ts = '2024-01-01T00:00:00Z'",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
			wantUp: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
		},
		{
			name: "eq then range", pk: []string{"ts", "id"},
			where: "WHERE ts = '2024-01-01T00:00:00Z' AND id >= 5",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z"), IntValue(5)}, Incl: true},
		},
		{
			name: "non-pk only", pk: []string{"ts"},
			where: "WHERE level = 'ERROR'",
			wantRng: false, wantExact: false,
		},
		{
			name: "mixed pk and non-pk", pk: []string{"ts"},
			where: "WHERE ts >= '2024-01-01T00:00:00Z' AND level = 'ERROR'",
			wantRng: true, wantExact: false,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
		},
		{
			name: "or prevents prune", pk: []string{"ts"},
			where: "WHERE ts >= '2024-01-01T00:00:00Z' OR level = 'ERROR'",
			wantRng: false, wantExact: false,
		},
		{
			name: "eq with conflicting range not exact", pk: []string{"ts"},
			where: "WHERE ts = '2024-01-01T00:00:00Z' AND ts >= '2024-02-01T00:00:00Z'",
			wantRng: true, wantExact: false,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
			wantUp: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
		},
		{
			name: "non-leading pk constrained", pk: []string{"ts", "id"},
			where: "WHERE id = 42",
			wantRng: false, wantExact: false,
		},
		{
			name: "reversed comparison", pk: []string{"ts"},
			where: "WHERE '2024-01-01T00:00:00Z' <= ts",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{mustTs("2024-01-01T00:00:00Z")}, Incl: true},
		},
		{
			name: "string pk bound", pk: []string{"id"},
			where: "WHERE id > 100",
			wantRng: true, wantExact: true,
			wantLo: &PKBound{Prefix: []Value{IntValue(100)}, Incl: false},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			where := selWhere(t, "SELECT * FROM logs "+tc.where)
			rng, exact := PKRangeFromWhere(pruneSchema(tc.pk...), where)
			if (rng != nil) != tc.wantRng {
				t.Fatalf("rng nil? got %v, want %v (rng=%+v)", rng != nil, tc.wantRng, rng)
			}
			if exact != tc.wantExact {
				t.Fatalf("exact=%v, want %v", exact, tc.wantExact)
			}
			if rng == nil {
				return
			}
			if !pkboundsEqual(rng.Lower, tc.wantLo) {
				t.Fatalf("lower=%+v, want %+v", rng.Lower, tc.wantLo)
			}
			if !pkboundsEqual(rng.Upper, tc.wantUp) {
				t.Fatalf("upper=%+v, want %+v", rng.Upper, tc.wantUp)
			}
		})
	}
}

func pkboundsEqual(a, b *PKBound) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Incl != b.Incl || len(a.Prefix) != len(b.Prefix) {
		return false
	}
	for i := range a.Prefix {
		c, err := Compare(a.Prefix[i], b.Prefix[i])
		if err != nil || c != 0 {
			return false
		}
	}
	return true
}

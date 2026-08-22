package storage_test

import (
	"strings"
	"testing"

	"ydbgo/internal/sql"
	"ydbgo/internal/storage"
)

func newSQLUpdateFixture(b *testing.B) *sql.Executor {
	e, err := storage.Open(b.TempDir() + "/db")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close() })
	ex := sql.NewExecutor(e)
	if stmts, err := sql.Parse("CREATE TABLE t (id int64 primary key, v string, g int64) ENGINE=CSTORE2"); err != nil {
		b.Fatal(err)
	} else if _, err := ex.Execute(stmts[0]); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < 100000; start += 8192 {
		end := start + 8192
		var sb strings.Builder
		sb.WriteString("INSERT INTO t (id, v, g) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			sb.WriteString(itoa(int64(i)))
			sb.WriteString(", 'v', ")
			sb.WriteString(itoa(int64(i)))
			sb.WriteString(")")
		}
		if stmts, err := sql.Parse(sb.String()); err != nil {
			b.Fatal(err)
		} else if _, err := ex.Execute(stmts[0]); err != nil {
			b.Fatal(err)
		}
	}
	return ex
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		buf[p] = '-'
	}
	return string(buf[p:])
}

func BenchmarkExecUpdateRange100k(b *testing.B) {
	ex := newSQLUpdateFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmts, err := sql.Parse("UPDATE t SET g = 7 WHERE id < 100000")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := ex.Execute(stmts[0]); err != nil {
			b.Fatal(err)
		}
	}
}

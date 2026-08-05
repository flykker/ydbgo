package server

import (
	"path/filepath"
	"testing"

	"ydbgo/internal/proto"
	"ydbgo/internal/storage"
)

func TestServerClientRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	eng, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	srv := NewServer(eng)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go srv.Serve()

	c, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	mustOK := func(sql string) *proto.Response {
		t.Helper()
		r, err := c.Execute(sql)
		if err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
		if !r.OK {
			t.Fatalf("execute %q: %s", sql, r.Error)
		}
		return r
	}

	mustOK("CREATE TABLE users (id int64 primary key, name string, age int64)")
	mustOK("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")

	r := mustOK("SELECT name, age FROM users WHERE age >= 30 ORDER BY id")
	if len(r.Result.Rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(r.Result.Rows))
	}
	if r.Result.Rows[0][0] != "Bob" {
		t.Errorf("got %v", r.Result.Rows[0])
	}

	// error propagation
	r, err = c.Execute("SELECT * FROM missing_table")
	if err != nil {
		t.Fatal(err)
	}
	if r.OK {
		t.Fatal("expected error for missing table")
	}
	if r.Error == "" {
		t.Fatal("expected non-empty error message")
	}

	// multiple statements in one request
	r = mustOK("UPDATE users SET age = 31 WHERE name = 'Bob'; SELECT age FROM users WHERE name = 'Bob'")
	if r.Result.Rows[0][0] != "31" {
		t.Errorf("last result=%v", r.Result.Rows)
	}
}

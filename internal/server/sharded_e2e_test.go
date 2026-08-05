package server

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"ydbgo/internal/proto"
	"ydbgo/internal/shard"
)

type shardedNode struct {
	c    *proto.Client
	addr string
	stop func()
}

func freeRaftPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startShardedNode(t *testing.T, dir string, rf int, shardSize uint64, splitTick time.Duration) *shardedNode {
	t.Helper()
	raftAddr := freeRaftPort(t)
	mgr, err := shard.NewManager(shard.Config{
		ID:        "n1",
		SQLAddr:   "127.0.0.1:0",
		RaftAddr:  raftAddr,
		DataDir:   dir,
		RF:        rf,
		ShardSize: shardSize,
		SplitTick: splitTick,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Start(true, ""); err != nil {
		mgr.Close()
		t.Fatalf("manager start: %v", err)
	}
	srv := NewShardedServer(mgr)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		mgr.Close()
		t.Fatal(err)
	}
	go srv.Serve()
	c, err := Dial(srv.Addr().String())
	if err != nil {
		mgr.Close()
		t.Fatal(err)
	}
	return &shardedNode{
		c:    c,
		addr: srv.Addr().String(),
		stop: func() { c.Close(); srv.Close(); mgr.Close() },
	}
}

func execOK(t *testing.T, c *proto.Client, sql string) *proto.Response {
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

func TestShardedSingleNodeFlow(t *testing.T) {
	n := startShardedNode(t, t.TempDir(), 0, 0, 0)
	defer n.stop()
	c := n.c

	execOK(t, c, "CREATE TABLE users (id int64 primary key, name string)")
	execOK(t, c, "INSERT INTO users VALUES (1,'a'),(2,'b'),(3,'c')")

	r := execOK(t, c, "SELECT * FROM users ORDER BY id")
	if len(r.Result.Rows) != 3 {
		t.Fatalf("rows=%d want 3: %v", len(r.Result.Rows), r.Result.Rows)
	}
	r = execOK(t, c, "SELECT name FROM users WHERE id = 2")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "b" {
		t.Fatalf("where rows=%v", r.Result.Rows)
	}
	execOK(t, c, "UPDATE users SET name = 'zz' WHERE id = 1")
	r = execOK(t, c, "SELECT name FROM users WHERE id = 1")
	if r.Result.Rows[0][0] != "zz" {
		t.Fatalf("update rows=%v", r.Result.Rows)
	}

	r = execOK(t, c, "ADMIN SHARDS users")
	if len(r.Result.Rows) != 1 {
		t.Fatalf("expected 1 shard, got %v", r.Result.Rows)
	}

	execOK(t, c, "ADMIN SPLIT TABLE users AT 2")

	r = execOK(t, c, "SELECT * FROM users ORDER BY id")
	if len(r.Result.Rows) != 3 {
		t.Fatalf("after split rows=%d want 3: %v", len(r.Result.Rows), r.Result.Rows)
	}
	r = execOK(t, c, "ADMIN SHARDS users")
	if len(r.Result.Rows) != 2 {
		t.Fatalf("expected 2 shards after split, got %v", r.Result.Rows)
	}

	execOK(t, c, "INSERT INTO users VALUES (10,'x'),(-5,'neg')")
	r = execOK(t, c, "SELECT count(id) FROM users")
	if r.Result.Rows[0][0] != "5" {
		t.Fatalf("count=%s want 5", r.Result.Rows[0][0])
	}
}

func TestShardedAutoSplit(t *testing.T) {
	c := startShardedNode(t, t.TempDir(), 0, 128, 100*time.Millisecond).c

	execOK(t, c, "CREATE TABLE t (id int64 primary key, v string)")
	vals := make([]string, 50)
	for i := 0; i < 50; i++ {
		vals[i] = fmt.Sprintf("(%d,'%d')", i, i)
	}
	execOK(t, c, "INSERT INTO t VALUES "+strings.Join(vals, ","))

	deadline := time.Now().Add(15 * time.Second)
	shards := 0
	for time.Now().Before(deadline) {
		r := execOK(t, c, "ADMIN SHARDS t")
		shards = len(r.Result.Rows)
		if shards >= 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if shards < 2 {
		t.Fatalf("auto-split did not trigger, shards=%d", shards)
	}
	r := execOK(t, c, "SELECT count(id) FROM t")
	if r.Result.Rows[0][0] != "50" {
		t.Fatalf("count=%s want 50", r.Result.Rows[0][0])
	}
}

func TestShardedPersistence(t *testing.T) {
	dir := t.TempDir()
	n := startShardedNode(t, dir, 0, 0, 0)
	execOK(t, n.c, "CREATE TABLE p (id int64 primary key, v string)")
	execOK(t, n.c, "INSERT INTO p VALUES (1,'a'),(2,'b')")
	execOK(t, n.c, "ADMIN SPLIT TABLE p AT 2")
	n.stop()

	// restart from the same data dir; shards must be re-mounted
	n2 := startShardedNode(t, dir, 0, 0, 0)
	defer n2.stop()
	r := execOK(t, n2.c, "SELECT * FROM p ORDER BY id")
	if len(r.Result.Rows) != 2 {
		t.Fatalf("rows=%d want 2 after restart: %v", len(r.Result.Rows), r.Result.Rows)
	}
	r = execOK(t, n2.c, "ADMIN SHARDS p")
	if len(r.Result.Rows) != 2 {
		t.Fatalf("shards=%d want 2 after restart", len(r.Result.Rows))
	}
}

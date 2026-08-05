package server

import (
	"net"
	"testing"
	"time"

	"ydbgo/internal/proto"
	"ydbgo/internal/shard"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

type shardNode struct {
	id   string
	c    *proto.Client
	mgr  *shard.Manager
	stop func()
	raf1 string
	sql1 string
}

func startClusterNode(t *testing.T, id string, raftAddr string, joinSQL string, rf int) *shardNode {
	return startClusterNodeR(t, id, raftAddr, joinSQL, rf, 0)
}

// startClusterNodeR is startClusterNode with a non-zero replica-heal interval.
func startClusterNodeR(t *testing.T, id string, raftAddr string, joinSQL string, rf int, recoveryTick time.Duration) *shardNode {
	t.Helper()
	sqlAddr := freePort(t)
	mgr, err := shard.NewManager(shard.Config{
		ID:           id,
		SQLAddr:      sqlAddr,
		RaftAddr:     raftAddr,
		DataDir:      t.TempDir(),
		RF:           rf,
		RecoveryTick: recoveryTick,
	})
	if err != nil {
		t.Fatal(err)
	}
	boot := joinSQL == ""
	if err := mgr.Start(boot, joinSQL); err != nil {
		mgr.Close()
		t.Fatalf("%s start: %v", id, err)
	}
	srv := NewShardedServer(mgr)
	if err := srv.Listen(sqlAddr); err != nil {
		mgr.Close()
		t.Fatal(err)
	}
	go srv.Serve()
	c, err := Dial(sqlAddr)
	if err != nil {
		mgr.Close()
		t.Fatal(err)
	}
	return &shardNode{id: id, c: c, mgr: mgr, sql1: sqlAddr,
		stop: func() { c.Close(); srv.Close(); mgr.Close() }}
}

func waitQuery(t *testing.T, c *proto.Client, sql string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		r, err := c.Execute(sql)
		if err == nil && r.OK && len(r.Result.Rows) == 1 {
			last = r.Result.Rows[0][0]
			if last == want {
				return
			}
		} else if err == nil && r.OK {
			if len(r.Result.Rows) > 0 {
				last = r.Result.Rows[0][0]
			} else {
				last = "<empty>"
			}
		} else if err != nil {
			last = "err:" + err.Error()
		} else {
			last = "err:" + r.Error
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%q never became %q (last=%s)", sql, want, last)
}

func TestShardedClusterReplication(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE t (id int64 primary key, v string)")
	execOK(t, n1.c, "INSERT INTO t VALUES (1, 'hello'), (2, 'world')")

	// rows visible from BOTH nodes once the shard group replicates
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM t", "2", 15*time.Second)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM t", "2", 15*time.Second)

	// write via n2, read via n1
	execOK(t, n2.c, "INSERT INTO t VALUES (3, 'three')")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM t", "3", 15*time.Second)

	// read by key via n2
	r := execOK(t, n2.c, "SELECT v FROM t WHERE id = 2")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "world" {
		t.Fatalf("select via n2: %v", r.Result.Rows)
	}

	// shard group has both nodes
	sh := execOK(t, n1.c, "ADMIN SHARDS t")
	t.Logf("shards: %v", sh.Result.Rows)
	if len(sh.Result.Rows) != 1 || !containsStr(sh.Result.Rows[0][3], "n1") || !containsStr(sh.Result.Rows[0][3], "n2") {
		t.Fatalf("placement: %v", sh.Result.Rows)
	}

	// split on the 2-node cluster
	execOK(t, n1.c, "ADMIN SPLIT TABLE t AT 2")
	sh2 := execOK(t, n1.c, "ADMIN SHARDS t")
	if len(sh2.Result.Rows) != 2 {
		t.Fatalf("after split: %v", sh2.Result.Rows)
	}
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM t", "3", 15*time.Second)
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM t", "3", 15*time.Second)

	// new writes land in the new shard, still visible
	execOK(t, n2.c, "INSERT INTO t VALUES (9, 'nine')")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM t", "4", 15*time.Second)
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

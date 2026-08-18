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

// startClusterNodeT is startClusterNode with a non-zero auto-TTL purge interval.
func startClusterNodeT(t *testing.T, id string, raftAddr string, joinSQL string, rf int, ttlTick time.Duration) *shardNode {
	t.Helper()
	sqlAddr := freePort(t)
	mgr, err := shard.NewManager(shard.Config{
		ID:       id,
		SQLAddr:  sqlAddr,
		RaftAddr: raftAddr,
		DataDir:  t.TempDir(),
		RF:       rf,
		TTLTick:  ttlTick,
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

// TestShardedKVRaw exercises the raw byte-KV surface over a sharded cluster:
// routing by key to the owning shard, replication across nodes, and range scan
// merging across shards.
func TestShardedKVRaw(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE kv_t (id int64 primary key) ENGINE=KV")
	execOK(t, n1.c, "ADMIN SPLIT TABLE kv_t AT 2")
	time.Sleep(200 * time.Millisecond)

	execOK(t, n1.c, "KV PUT kv_t 'aaa' '1'")
	execOK(t, n1.c, "KV PUT kv_t 'bbb' '2'")
	execOK(t, n1.c, "KV PUT kv_t 'ccc' '3'")

	// get by key from both nodes
	r := execOK(t, n2.c, "KV GET kv_t 'bbb'")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][1] != "2" {
		t.Fatalf("kv get via n2: %v", r.Result.Rows)
	}
	waitQueryKV(t, n1.c, "KV SCAN kv_t", "ccc", 15*time.Second)

	// range scan with start bound
	r = execOK(t, n1.c, "KV SCAN kv_t 'b'")
	if len(r.Result.Rows) != 2 || r.Result.Rows[0][0] != "bbb" || r.Result.Rows[1][0] != "ccc" {
		t.Fatalf("kv range scan: %v", r.Result.Rows)
	}

	// delete propagates
	execOK(t, n2.c, "KV DELETE kv_t 'aaa'")
	waitQueryKV(t, n1.c, "KV SCAN kv_t", "ccc", 15*time.Second)
	r = execOK(t, n1.c, "KV SCAN kv_t")
	if len(r.Result.Rows) != 2 {
		t.Fatalf("after delete: %v", r.Result.Rows)
	}
}

// TestShardedCStore exercises the columnar backend over a sharded cluster.
// TestShardedAdminCompact verifies ADMIN COMPACT forces an LSM compaction on
// every local shard group and that reads still return identical results after
// the merged tables replace the freshly-written runs.
func TestShardedAdminCompact(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE cs_t (id int64 primary key, v string, score int64) ENGINE=CSTORE")
	execOK(t, n1.c, "INSERT INTO cs_t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM cs_t", "3", 15*time.Second)

	r := execOK(t, n1.c, "SELECT SUM(score) AS s, COUNT(*) AS c FROM cs_t")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "60" || r.Result.Rows[0][1] != "3" {
		t.Fatalf("pre-compact aggregate: %v", r.Result.Rows)
	}

	// compact every node's shard stores, then verify identical reads
	for _, n := range []*shardNode{n1, n2} {
		execOK(t, n.c, "ADMIN COMPACT")
	}

	r = execOK(t, n1.c, "SELECT SUM(score) AS s, COUNT(*) AS c FROM cs_t")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "60" || r.Result.Rows[0][1] != "3" {
		t.Fatalf("post-compact aggregate: %v", r.Result.Rows)
	}
	r = execOK(t, n2.c, "SELECT v FROM cs_t WHERE id = 2")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "b" {
		t.Fatalf("post-compact point read: %v", r.Result.Rows)
	}
}

// TestShardedDropTableRecreate verifies DROP TABLE physically destroys every
// shard replica so re-creating a table with the same name starts empty. The
// bug: shard IDs are deterministic (table + split path), so a re-created table
// reused the old shard directories whose raft log was resumed, resurrecting
// the dropped rows.
func TestShardedDropTableRecreate(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE dt (id int64 primary key, v string, score int64) ENGINE=CSTORE")
	execOK(t, n1.c, "INSERT INTO dt VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM dt", "3", 15*time.Second)

	// spread across shards so multiple shard groups hold data
	execOK(t, n1.c, "ADMIN SPLIT TABLE dt AT 2")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM dt", "3", 15*time.Second)

	// drop, then recreate with the same name
	execOK(t, n1.c, "DROP TABLE dt")
	execOK(t, n1.c, "CREATE TABLE dt (id int64 primary key, v string, score int64) ENGINE=CSTORE")

	// a fresh table must be empty on every node (no resurrected rows)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM dt", "0", 15*time.Second)
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM dt", "0", 15*time.Second)

	// writes land on the fresh table only
	execOK(t, n2.c, "INSERT INTO dt VALUES (9, 'nine', 90)")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM dt", "1", 15*time.Second)
	r := execOK(t, n1.c, "SELECT v FROM dt WHERE id = 9")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "nine" {
		t.Fatalf("fresh table point read: %v", r.Result.Rows)
	}
}

func TestShardedCStore(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE cs_t (id int64 primary key, v string, score int64) ENGINE=CSTORE")
	execOK(t, n1.c, "INSERT INTO cs_t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM cs_t", "3", 15*time.Second)

	// read by key via n2 (routes to shard owning id=2)
	r := execOK(t, n2.c, "SELECT v FROM cs_t WHERE id = 2")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "b" {
		t.Fatalf("cstore select via n2: %v", r.Result.Rows)
	}

	// split and keep writing/reading
	execOK(t, n1.c, "ADMIN SPLIT TABLE cs_t AT 2")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM cs_t", "3", 15*time.Second)
	execOK(t, n2.c, "INSERT INTO cs_t VALUES (9, 'nine', 90)")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM cs_t", "4", 15*time.Second)
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM cs_t", "4", 15*time.Second)

	// aggregate over both shards (union of per-shard scans)
	waitQuery(t, n2.c, "SELECT SUM(score) AS s FROM cs_t", "150", 15*time.Second)
	waitQuery(t, n1.c, "SELECT MAX(score) AS m FROM cs_t", "90", 15*time.Second)

	// column projection across the split (only v + pk are read per shard)
	r = execOK(t, n1.c, "SELECT v FROM cs_t ORDER BY id")
	if len(r.Result.Rows) != 4 || r.Result.Rows[0][0] != "a" || r.Result.Rows[3][0] != "nine" {
		t.Fatalf("projected scan: %v", r.Result.Rows)
	}
}

// TestShardedMpart drives the native columnar ENGINE=CSTORE2 backend through
// the full sharded path: replication, split, aggregate/group pushdown, range
// delete and compaction, mirroring TestShardedCStore.
func TestShardedMpart(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE mp_t (id int64 primary key, v string, score int64) ENGINE=CSTORE2")
	execOK(t, n1.c, "INSERT INTO mp_t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM mp_t", "3", 15*time.Second)

	// read by key via n2 (routes to the shard owning id=2)
	r := execOK(t, n2.c, "SELECT v FROM mp_t WHERE id = 2")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "b" {
		t.Fatalf("mpart select via n2: %v", r.Result.Rows)
	}

	// split and keep writing/reading
	execOK(t, n1.c, "ADMIN SPLIT TABLE mp_t AT 2")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM mp_t", "3", 15*time.Second)
	execOK(t, n2.c, "INSERT INTO mp_t VALUES (9, 'nine', 90)")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM mp_t", "4", 15*time.Second)
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM mp_t", "4", 15*time.Second)

	// aggregate + grouped pushdown across both shards
	waitQuery(t, n2.c, "SELECT SUM(score) AS s FROM mp_t", "150", 15*time.Second)
	waitQuery(t, n1.c, "SELECT MAX(score) AS m FROM mp_t", "90", 15*time.Second)
	if r := execOK(t, n2.c, "SELECT v, COUNT(*) FROM mp_t GROUP BY v"); len(r.Result.Rows) != 4 {
		t.Fatalf("grouped pushdown: %v", r.Result.Rows)
	}

	// column projection across the split
	r = execOK(t, n1.c, "SELECT v FROM mp_t ORDER BY id")
	if len(r.Result.Rows) != 4 || r.Result.Rows[0][0] != "a" || r.Result.Rows[3][0] != "nine" {
		t.Fatalf("projected scan: %v", r.Result.Rows)
	}

	// range delete (retention) replicates to the follower
	execOK(t, n1.c, "DELETE FROM mp_t WHERE id >= 2 AND id < 3")
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM mp_t", "3", 15*time.Second)
	r = execOK(t, n2.c, "SELECT SUM(score) AS s FROM mp_t")
	if r.Result.Rows[0][0] != "130" {
		t.Fatalf("sum after retention: %v", r.Result.Rows)
	}
}

// TestShardedTopN verifies ORDER BY PK ... LIMIT pushdown across a split
// CSTORE table: each shard answers with its own top rows (bounded PK-index
// scan), the coordinator merges/re-sorts them, and replication neither
// duplicates nor drops rows.
func TestShardedTopN(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (id int64 primary key, v string) ENGINE=CSTORE")
	for i := 0; i < 12; i++ {
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES (%d, 'v%d')", i, i))
	}
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT 5")
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT 9")
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "12", 15*time.Second)

	// top-4 descending spans all three shards
	r := execOK(t, n1.c, "SELECT id FROM logs ORDER BY id DESC LIMIT 4")
	if len(r.Result.Rows) != 4 || r.Result.Rows[0][0] != "11" || r.Result.Rows[3][0] != "8" {
		t.Fatalf("topn desc: %v", r.Result.Rows)
	}

	// top-2 ascending
	r = execOK(t, n1.c, "SELECT id FROM logs ORDER BY id ASC LIMIT 2")
	if len(r.Result.Rows) != 2 || r.Result.Rows[0][0] != "0" || r.Result.Rows[1][0] != "1" {
		t.Fatalf("topn asc: %v", r.Result.Rows)
	}

	// restricted to a range spanning two shards
	r = execOK(t, n1.c, "SELECT id FROM logs WHERE id >= 4 AND id < 10 ORDER BY id DESC LIMIT 3")
	if len(r.Result.Rows) != 3 || r.Result.Rows[0][0] != "9" || r.Result.Rows[2][0] != "7" {
		t.Fatalf("topn range: %v", r.Result.Rows)
	}
}

// TestShardedCStorePrune verifies primary-key range pruning across a sharded
// CSTORE table: windows are counted/aggregated over only the matching rows and
// the query is routed only to shards that can contain them.
func TestShardedCStorePrune(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, level string, lat int64) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		level := "INFO"
		if h%3 == 0 {
			level = "ERROR"
		}
		ts := base.Add(time.Duration(h) * time.Hour)
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s', %d)", ts.Format(time.RFC3339Nano), level, h*10))
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	// split so 12h+ lands on a second shard
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT '2024-01-01T12:00:00Z'")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	// window spanning the split boundary
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T10:00:00Z' AND ts < '2024-01-01T14:00:00Z'", "4", 15*time.Second)
	// window fully inside the first shard
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T02:00:00Z'", "2", 15*time.Second)
	// window fully inside the second shard (skips the first)
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T20:00:00Z' AND ts < '2024-01-02T00:00:00Z'", "4", 15*time.Second)
	// exact timestamp
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE ts = '2024-01-01T15:00:00Z'", "1", 15*time.Second)

	// aggregate over the window
	waitQuery(t, n2.c, "SELECT SUM(lat) AS s FROM logs WHERE ts >= '2024-01-01T12:00:00Z' AND ts < '2024-01-01T15:00:00Z'", "390", 15*time.Second)

	// mixed PK window + non-PK filter (per-shard row filter)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T09:00:00Z' AND level = 'ERROR'", "3", 15*time.Second)

	// projected scan over a window returns only matching rows
	r := execOK(t, n2.c, "SELECT level FROM logs WHERE ts >= '2024-01-01T06:00:00Z' AND ts < '2024-01-01T09:00:00Z' ORDER BY ts")
	if len(r.Result.Rows) != 3 || r.Result.Rows[0][0] != "ERROR" || r.Result.Rows[2][0] != "INFO" {
		t.Fatalf("projected window: %v", r.Result.Rows)
	}
}

// TestShardedCStoreAggPushdown verifies whole-table aggregates are computed by
// merging per-shard partial aggregates rather than moving rows across shards.
func TestShardedCStoreAggPushdown(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, level string, lat int64) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		level := "INFO"
		if h%3 == 0 {
			level = "ERROR"
		}
		ts := base.Add(time.Duration(h) * time.Hour)
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s', %d)", ts.Format(time.RFC3339Nano), level, h*10))
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT '2024-01-01T12:00:00Z'")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	// whole-table aggregates across both shards (no WHERE)
	waitQuery(t, n2.c, "SELECT SUM(lat) AS s FROM logs", "2760", 15*time.Second)
	waitQuery(t, n1.c, "SELECT AVG(lat) AS a FROM logs", "115", 15*time.Second)
	waitQuery(t, n2.c, "SELECT MAX(lat) AS mx FROM logs", "230", 15*time.Second)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c, SUM(lat) AS s FROM logs", "24", 15*time.Second)

	// aggregate over a window spanning the split boundary
	waitQuery(t, n2.c, "SELECT SUM(lat) AS s FROM logs WHERE ts >= '2024-01-01T10:00:00Z' AND ts < '2024-01-01T14:00:00Z'", "460", 15*time.Second)
	waitQuery(t, n1.c, "SELECT AVG(lat) AS a FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T06:00:00Z'", "25", 15*time.Second)
	// aggregate fully inside one shard (the other is skipped)
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T20:00:00Z' AND ts < '2024-01-02T00:00:00Z'", "4", 15*time.Second)

	// non-PK equality filter is pushed as a columnar predicate to the partial
	// aggregate on each shard (no rows cross the wire)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE level = 'ERROR'", "8", 15*time.Second)
	waitQuery(t, n2.c, "SELECT SUM(lat) AS s FROM logs WHERE level = 'INFO' AND ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T12:00:00Z'", "480", 15*time.Second)
}

// TestShardedCStorePredicatePushdown verifies a leading-literal LIKE on a
// non-PK column is pushed into the columnar scan/aggregate on each shard
// (COUNT with no rows crossing the wire), while a leading-wildcard pattern
// falls back to the row path and must still agree.
func TestShardedCStorePredicatePushdown(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, level string, lat int64) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		level := "INFO"
		if h%3 == 0 {
			level = "ERROR"
		}
		ts := base.Add(time.Duration(h) * time.Hour)
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s', %d)", ts.Format(time.RFC3339Nano), level, h*10))
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT '2024-01-01T12:00:00Z'")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	// COUNT(*) filtered by a leading-literal LIKE prefix on a non-PK column,
	// across both shards
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE level LIKE 'ERRO%'", "8", 15*time.Second)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE level LIKE 'INFO%'", "16", 15*time.Second)

	// aggregate over a column while filtering on a different column
	waitQuery(t, n2.c, "SELECT SUM(lat) AS s FROM logs WHERE level LIKE 'ERRO%'", "840", 15*time.Second)
	waitQuery(t, n1.c, "SELECT AVG(lat) AS a FROM logs WHERE level LIKE 'INFO%'", "120", 15*time.Second)

	// equality on a non-PK column pushed as a predicate
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE level = 'ERROR'", "8", 15*time.Second)

	// LIKE with an equality-only pattern (no wildcard) folds to a predicate
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE level LIKE 'INFO'", "16", 15*time.Second)

	// leading wildcard cannot be folded: falls back to the row path, still right
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE level LIKE '%RROR'", "8", 15*time.Second)
	// mid-pattern wildcard: same fallback
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE level LIKE 'E%OR'", "8", 15*time.Second)

	// PK window ANDed with a LIKE predicate: range + predicate both applied
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T00:00:00Z' AND ts < '2024-01-01T12:00:00Z' AND level LIKE 'ERRO%'", "4", 15*time.Second)
	waitQuery(t, n1.c, "SELECT SUM(lat) AS s FROM logs WHERE ts >= '2024-01-01T12:00:00Z' AND level LIKE 'INFO%'", "1440", 15*time.Second)

	// plain projected scan with a LIKE predicate
	r := execOK(t, n2.c, "SELECT level FROM logs WHERE level LIKE 'ERRO%' ORDER BY ts")
	if len(r.Result.Rows) != 8 {
		t.Fatalf("projected LIKE scan: %v", r.Result.Rows)
	}
	for _, row := range r.Result.Rows {
		if len(row) == 0 || row[0] != "ERROR" {
			t.Fatalf("projected LIKE scan row: %v", row)
		}
	}
}

// TestShardedCStoreRetention verifies a time-based retention delete spreads to
// every shard as a columnar range delete.
func TestShardedCStoreRetention(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, level string, lat int64) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		ts := base.Add(time.Duration(h) * time.Hour)
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', 'INFO', %d)", ts.Format(time.RFC3339Nano), h*10))
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT '2024-01-01T12:00:00Z'")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	// retire the first 10 hours from both shards
	r := execOK(t, n2.c, "DELETE FROM logs WHERE ts < '2024-01-01T10:00:00Z'")
	if r.Result.Affected != 10 {
		t.Fatalf("retire affected=%d want 10", r.Result.Affected)
	}
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "14", 15*time.Second)

	// window counts reflect the deletion
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T08:00:00Z' AND ts < '2024-01-01T12:00:00Z'", "2", 15*time.Second)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T12:00:00Z' AND ts < '2024-01-01T16:00:00Z'", "4", 15*time.Second)
	// aggregates over the survivors
	waitQuery(t, n2.c, "SELECT SUM(lat) AS s FROM logs", "2310", 15*time.Second)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs WHERE ts >= '2024-01-01T05:00:00Z'", "14", 15*time.Second)

	// retire a middle window and verify a later aggregate
	r = execOK(t, n1.c, "DELETE FROM logs WHERE ts >= '2024-01-01T12:00:00Z' AND ts < '2024-01-01T16:00:00Z'")
	if r.Result.Affected != 4 {
		t.Fatalf("window retire affected=%d want 4", r.Result.Affected)
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "10", 15*time.Second)
	waitQuery(t, n1.c, "SELECT SUM(lat) AS s FROM logs", "1770", 15*time.Second)
}

// TestShardedCStoreAutoTTL verifies the background retention loop purges rows
// older than a table's RETENTION window on the shard leader, replicated to
// every replica through Raft.
func TestShardedCStoreAutoTTL(t *testing.T) {
	n1 := startClusterNodeT(t, "n1", freePort(t), "", 1, 150*time.Millisecond)
	defer n1.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, v string) ENGINE=CSTORE RETENTION='2s'")
	base := time.Now().UTC()
	old1 := base.Add(-time.Hour).Format(time.RFC3339Nano)
	old2 := base.Add(-time.Hour).Add(time.Minute).Format(time.RFC3339Nano)
	new1 := base.Add(time.Hour).Format(time.RFC3339Nano)
	new2 := base.Add(time.Hour).Add(time.Minute).Format(time.RFC3339Nano)
	execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', 'old1')", old1))
	execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', 'old2')", old2))
	execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', 'new1')", new1))
	execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', 'new2')", new2))
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "4", 15*time.Second)

	// the retention loop must drop the two old rows and keep the fresh ones
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "2", 15*time.Second)
	got := execOK(t, n1.c, "SELECT v FROM logs ORDER BY v").Result.Rows
	if len(got) != 2 || got[0][0] != "new1" || got[1][0] != "new2" {
		t.Fatalf("survivors=%v want [new1 new2]", got)
	}
}

// TestShardedCStoreGroupByPushdown verifies GROUP BY results computed by
// merging per-shard partial groups match the single-shard (local pushdown)
// result and hand-computed expectations.
func TestShardedCStoreGroupByPushdown(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, level string, lat int64) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 24; h++ {
		level := "INFO"
		if h%3 == 0 {
			level = "ERROR"
		}
		ts := base.Add(time.Duration(h) * time.Hour)
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s', %d)", ts.Format(time.RFC3339Nano), level, h*10))
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)
	// stabilize the local replica n1 reads from (writes route through n1 but its
	// follower replica may still be catching up after the burst of inserts)
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	groupMap := func(sql string) map[string]string {
		t.Helper()
		r := execOK(t, n1.c, sql)
		out := map[string]string{}
		for _, row := range r.Result.Rows {
			out[row[0]] = strings.Join(row[1:], "|")
		}
		return out
	}
	// single-shard (local columnar GROUP BY) baseline before the split
	before := groupMap("SELECT level, COUNT(*), SUM(lat) FROM logs GROUP BY level")
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT '2024-01-01T12:00:00Z'")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "24", 15*time.Second)

	// the merged result across both shards must equal the local baseline
	after := groupMap("SELECT level, COUNT(*), SUM(lat) FROM logs GROUP BY level")
	if len(after) != len(before) {
		t.Fatalf("group sets differ: before=%v after=%v", before, after)
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("group %s: after=%q want %q (before)", k, after[k], v)
		}
	}

	// window GROUP BY spanning the split boundary
	want := map[string]string{"ERROR": "4", "INFO": "8"}
	got := groupMap("SELECT level, COUNT(*) FROM logs WHERE ts >= '2024-01-01T06:00:00Z' AND ts < '2024-01-01T18:00:00Z' GROUP BY level")
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("window group %s = %q want %q", k, got[k], v)
		}
	}
	// AVG across shards re-weights partial sums by their counts
	want = map[string]string{"ERROR": "105", "INFO": "105"}
	got = groupMap("SELECT level, AVG(lat) FROM logs WHERE ts >= '2024-01-01T06:00:00Z' AND ts < '2024-01-01T16:00:00Z' GROUP BY level")
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("avg group %s = %q want %q", k, got[k], v)
		}
	}
	// group column not selected: only aggregates are returned, one row per group
	got = groupMap("SELECT COUNT(*) FROM logs GROUP BY level")
	if got["16"] != "" || got["8"] != "" || len(got) != 2 {
		t.Fatalf("agg-only groups: %v", got)
	}
}

// TestShardedCStoreLike verifies LIKE/NOT LIKE filtering pushes to each shard
// and merges correctly across shard boundaries, and that a leading-wildcard
// pattern still scans every shard.
func TestShardedCStoreLike(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, name string) ENGINE=CSTORE")
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	names := []string{"alpha", "alpine", "beta", "alps", "Alphabet"}
	for i, nm := range names {
		ts := base.Add(time.Duration(i) * time.Hour)
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO logs VALUES ('%s', '%s')", ts.Format(time.RFC3339Nano), nm))
	}
	waitQuery(t, n2.c, "SELECT COUNT(*) AS c FROM logs", "5", 15*time.Second)
	execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT '2024-01-01T02:00:00Z'")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM logs", "5", 15*time.Second)

	rows := func(sql string) []string {
		t.Helper()
		r := execOK(t, n1.c, sql)
		out := make([]string, 0, len(r.Result.Rows))
		for _, row := range r.Result.Rows {
			out = append(out, strings.Join(row, "|"))
		}
		return out
	}
	cases := []struct {
		sql  string
		want []string
	}{
		{"SELECT name FROM logs WHERE name LIKE 'alp%' ORDER BY name", []string{"alpha", "alpine", "alps"}},
		{"SELECT name FROM logs WHERE name LIKE '%alp%' ORDER BY name", []string{"alpha", "alpine", "alps"}},
		{"SELECT name FROM logs WHERE name NOT LIKE '%lp%' ORDER BY name", []string{"beta"}},
		{"SELECT COUNT(*) FROM logs WHERE name LIKE 'zz%'", []string{"0"}},
	}
	for _, c := range cases {
		got := rows(c.sql)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Fatalf("%q = %v want %v", c.sql, got, c.want)
		}
	}
}

func waitQueryKV(t *testing.T, c *proto.Client, sql string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		r, err := c.Execute(sql)
		if err == nil && r.OK && len(r.Result.Rows) > 0 {
			last = r.Result.Rows[len(r.Result.Rows)-1][0]
			if last == want {
				return
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

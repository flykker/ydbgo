package server

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ydbgo/internal/proto"
)

// TestBenchFiveNodeWrites drives the same concurrent batch-write workload as
// the etcd comparison bench against a real 5-node RF=3 sharded cluster, then
// checks that every node sees the total row count.
func TestBenchFiveNodeWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	const rf = 3
	n1 := startClusterNode(t, "n1", freePort(t), "", rf)
	defer n1.stop()
	nodes := []*shardNode{n1}
	for i := 2; i <= 5; i++ {
		n := startClusterNode(t, fmt.Sprintf("n%d", i), freePort(t), n1.sql1, rf)
		defer n.stop()
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		waitRegistered(t, n, 5, 15*time.Second)
	}

	execOK(t, n1.c, "CREATE TABLE t (id int64 primary key, v string)")

	// 4 shards -> replication across the cluster, like the 5-node test
	for _, at := range []string{"250", "500", "750"} {
		execOK(t, n1.c, fmt.Sprintf("ADMIN SPLIT TABLE t AT %s", at))
	}
	time.Sleep(300 * time.Millisecond)

	const statements = 120
	const rowsPer = 200
	const conc = 8
	pool := proto.NewConnPool(conc)
	defer pool.Close()

	var next int64
	var latMu sync.Mutex
	var lats []time.Duration
	var wg sync.WaitGroup
	var failures int64

	worker := func() {
		defer wg.Done()
		for {
			s := atomic.AddInt64(&next, 1) - 1
			if s >= statements {
				return
			}
			base := s * rowsPer
			q := "INSERT INTO t VALUES "
			for i := 0; i < rowsPer; i++ {
				if i > 0 {
					q += ","
				}
				q += fmt.Sprintf("(%d,'v%d')", base+int64(i), base+int64(i))
			}
			start := time.Now()
			err := pool.Do(n1.sql1, func(c *proto.Client) error {
				resp, e := c.Execute(q)
				if e != nil {
					return e
				}
				if !resp.OK {
					return fmt.Errorf("%s", resp.Error)
				}
				return nil
			})
			if err != nil {
				atomic.AddInt64(&failures, 1)
				continue
			}
			latMu.Lock()
			lats = append(lats, time.Since(start))
			latMu.Unlock()
		}
	}

	for i := 0; i < conc; i++ {
		wg.Add(1)
		go worker()
	}
	wg.Wait()

	if failures != 0 {
		t.Fatalf("%d write statements failed", failures)
	}
	latMu.Lock()
	d := make([]time.Duration, len(lats))
	copy(d, lats)
	latMu.Unlock()
	if len(d) == 0 {
		t.Fatal("no latencies recorded")
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	p := func(f float64) time.Duration {
		idx := int(f * float64(len(d)))
		if idx >= len(d) {
			idx = len(d) - 1
		}
		return d[idx]
	}
	p50, p99 := p(0.50), p(0.99)
	t.Logf("5-node rf=%d client write p50=%.3fms p99=%.3fms statements=%d", rf, float64(p50)/1e6, float64(p99)/1e6, len(d))

	total := statements * rowsPer
	for _, n := range nodes {
		n := n
		go waitAll(t, n, total) // FAIL is handled inside
	}
	// wait a moment for replication then check
	time.Sleep(3 * time.Second)
	for _, n := range nodes {
		r := execOK(t, n.c, "SELECT COUNT(*) AS c FROM t")
		if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != fmt.Sprintf("%d", total) {
			t.Fatalf("%s count=%v want %d", n.id, r.Result.Rows, total)
		}
	}
}

func waitAll(t *testing.T, n *shardNode, total int) {
	waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM t", fmt.Sprintf("%d", total), 30*time.Second)
}
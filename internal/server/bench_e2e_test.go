package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ydbgo/internal/proto"
)

// TestBenchConcurrentWrites is an in-process e2e benchmark: it drives a
// concurrent write workload at a sharded cluster over the wire protocol and
// checks both client-side latency and the server-side ADMIN METRICS p50/p99.
func TestBenchConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()

	execOK(t, n1.c, "CREATE TABLE t (id int64 primary key, v string)")

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
	t.Logf("client write p50=%.3fms p99=%.3fms statements=%d", float64(p50)/1e6, float64(p99)/1e6, len(d))
	if p99 <= 0 {
		t.Fatalf("expected p99 > 0, got %v", p99)
	}

	// verify all rows landed (statements*rowsPer)
	total := statements * rowsPer
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM t", fmt.Sprintf("%d", total), 20*time.Second)

	// server-side ADMIN METRICS must reflect the writes
	resp, err := n1.c.Execute("ADMIN METRICS")
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("ADMIN METRICS failed: %s", resp.Error)
	}
	if resp.Result == nil || resp.Result.Note == "" {
		t.Fatalf("ADMIN METRICS produced no output")
	}
	note := resp.Result.Note
	for _, want := range []string{"write_latency_ms", "p50=", "p99=", "writes="} {
		if !strings.Contains(note, want) {
			t.Errorf("ADMIN METRICS missing %q in: %s", want, note)
		}
	}
	t.Logf("server ADMIN METRICS:\n%s", note)
}

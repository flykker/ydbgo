package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ydbgo/internal/proto"
)

// TestStressStallWriteLoad reproduces the intermittent write stall observed at
// high volume: with 8 concurrent clients each issuing 200-row INSERTs, a
// statement occasionally takes >60s (client rpc timeout). On any request
// exceeding the stall threshold it dumps all goroutines for diagnosis.
func TestStressStallWriteLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	n1 := startClusterNode(t, "n1", freePort(t), "", 3)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 3)
	defer n2.stop()
	n3 := startClusterNode(t, "n3", freePort(t), n1.sql1, 3)
	defer n3.stop()
	n4 := startClusterNode(t, "n4", freePort(t), n1.sql1, 3)
	defer n4.stop()
	n5 := startClusterNode(t, "n5", freePort(t), n1.sql1, 3)
	defer n5.stop()

	execOK(t, n1.c, "CREATE TABLE logs (ts timestamp primary key, level string, lat double) ENGINE=CSTORE")
	for _, at := range []string{"'2024-01-01T01:04:00Z'", "'2024-01-01T00:32:00Z'", "'2024-01-01T02:08:00Z'", "'2024-01-01T03:12:00Z'"} {
		execOK(t, n1.c, "ADMIN SPLIT TABLE logs AT "+at)
	}

	const (
		clients    = 8
		perClient  = 700 // statements per client
		rows       = 200
		stallLimit = 30 * time.Second
	)
	pool := proto.NewConnPool(clients)
	defer pool.Close()

	var next int64
	var stalls int64
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perClient; i++ {
				stmt := atomic.AddInt64(&next, 1) - 1
				base := int(stmt) * rows
				q := ydbStressBatch(base, rows)
				done := make(chan struct{})
				go func() {
					defer close(done)
					err := pool.Do(n1.sql1, func(cl *proto.Client) error {
						resp, err := cl.Execute(q)
						if err != nil {
							return err
						}
						if !resp.OK {
							return fmt.Errorf("%s", resp.Error)
						}
						return nil
					})
					if err != nil {
						atomic.AddInt64(&stalls, 1)
						t.Errorf("write: %v", err)
					}
				}()
				select {
				case <-done:
				case <-time.After(stallLimit):
					atomic.AddInt64(&stalls, 1)
					buf := make([]byte, 2<<20)
					n := runtime.Stack(buf, true)
					t.Logf("STALL: request did not finish within %s\n%s", stallLimit, buf[:n])
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("stalls=%d total=%d", stalls, clients*perClient)
	if stalls > 0 {
		t.Fatalf("detected %d stalls", stalls)
	}
}

// ydbStressBatch mirrors cmd/benchcol's ydbBatch INSERT shape.
func ydbStressBatch(start, n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO logs VALUES ")
	for j := 0; j < n; j++ {
		ts := time.Unix(0, int64(start+j)*int64(time.Second)).AddDate(0, 0, 0)
		ts = ts.Add(time.Duration((start+j)%3600) * time.Millisecond)
		var lv string
		m := (start + j) % 100
		switch {
		case m < 60:
			lv = "info"
		case m < 85:
			lv = "warn"
		default:
			lv = "error"
		}
		if j > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "('%s','%s',%.6f)", ts.Format(time.RFC3339Nano), lv, 50.0)
	}
	return b.String()
}

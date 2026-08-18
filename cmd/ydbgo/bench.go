package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ydbgo/internal/config"
	"ydbgo/internal/proto"
)

// runBench drives a concurrent write workload against a running server and
// reports client-side p50/p99 write latency, plus the server's own metrics.
func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:2135", "server address")
	configPath := fs.String("config", "", "cluster config (default addr = bootstrap node)")
	statements := fs.Int("n", 10000, "number of write statements to run")
	rows := fs.Int("rows", 100, "rows per INSERT statement")
	conc := fs.Int("c", 8, "concurrency")
	table := fs.String("table", "bench", "table name")
	engine := fs.String("engine", "TABLE", "storage engine (TABLE, KV, CSTORE, CSTORE2)")
	fs.Parse(args)
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bench:", err)
			os.Exit(2)
		}
		if err := applyClientConfig(fs, cfg); err != nil {
			fmt.Fprintln(os.Stderr, "bench:", err)
			os.Exit(2)
		}
	}

	pool := proto.NewConnPool(*conc)
	defer pool.Close()

	mustExec := func(q string) {
		if err := pool.Do(*addr, func(c *proto.Client) error {
			resp, err := c.Execute(q)
			if err != nil {
				return err
			}
			if !resp.OK {
				return fmt.Errorf("%s", resp.Error)
			}
			return nil
		}); err != nil {
			fmt.Fprintf(os.Stderr, "bench: %v\n", err)
			os.Exit(1)
		}
	}
	if *engine == "" || *engine == "TABLE" {
		mustExec(fmt.Sprintf("CREATE TABLE %s (id int64 primary key, v string, g int64)", *table))
	} else {
		mustExec(fmt.Sprintf("CREATE TABLE %s (id int64 primary key, v string, g int64) ENGINE=%s", *table, *engine))
	}
	mustExec(fmt.Sprintf("DELETE FROM %s", *table))

	var next int64
	var latMu sync.Mutex
	var lats []time.Duration
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for {
			stmt := atomic.AddInt64(&next, 1) - 1
			if stmt >= int64(*statements) {
				return
			}
			base := stmt * int64(*rows)
			q := buildBatch(*table, base, *rows)
			start := time.Now()
			if err := pool.Do(*addr, func(c *proto.Client) error {
				resp, err := c.Execute(q)
				if err != nil {
					return err
				}
				if !resp.OK {
					return fmt.Errorf("%s", resp.Error)
				}
				return nil
			}); err != nil {
				fmt.Fprintf(os.Stderr, "bench: %v\n", err)
				os.Exit(1)
			}
			latMu.Lock()
			lats = append(lats, time.Since(start))
			latMu.Unlock()
		}
	}

	start := time.Now()
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go worker()
	}
	wg.Wait()
	elapsed := time.Since(start)

	latMu.Lock()
	d := make([]time.Duration, len(lats))
	copy(d, lats)
	latMu.Unlock()
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	p := func(f float64) time.Duration {
		if len(d) == 0 {
			return 0
		}
		idx := int(f * float64(len(d)))
		if idx >= len(d) {
			idx = len(d) - 1
		}
		return d[idx]
	}
	var sum time.Duration
	for _, x := range d {
		sum += x
	}
	avg := time.Duration(0)
	if len(d) > 0 {
		avg = sum / time.Duration(len(d))
	}
	totalRows := int64(*statements) * int64(*rows)
	fmt.Printf("bench: %d statements x %d rows = %d rows, conc=%d\n", *statements, *rows, totalRows, *conc)
	fmt.Printf("elapsed: %s  (%.0f rows/s, %.0f stmt/s)\n",
		elapsed, float64(totalRows)/elapsed.Seconds(), float64(len(d))/elapsed.Seconds())
	fmt.Printf("client write latency: p50=%.3fms p99=%.3fms avg=%.3fms\n",
		float64(p(0.50))/1e6, float64(p(0.99))/1e6, float64(avg)/1e6)

	// server-side view
	var note string
	if err := pool.Do(*addr, func(c *proto.Client) error {
		resp, err := c.Execute("ADMIN METRICS")
		if err != nil {
			return err
		}
		if resp.OK && resp.Result != nil {
			note = resp.Result.Note
		}
		return nil
	}); err == nil && note != "" {
		fmt.Println("server metrics:")
		fmt.Println(note)
	}
}

// buildBatch renders an INSERT with `rows` consecutive ids starting at base
// and a low-cardinality group column g = id % 100 (drives GROUP BY pushdown).
func buildBatch(table string, base int64, rows int) string {
	q := "INSERT INTO " + table + " VALUES "
	for i := 0; i < rows; i++ {
		if i > 0 {
			q += ","
		}
		q += fmt.Sprintf("(%d,'v%d',%d)", base+int64(i), base+int64(i), (base+int64(i))%100)
	}
	return q
}

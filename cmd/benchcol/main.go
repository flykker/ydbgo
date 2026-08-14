// Command benchcol drives an identical log-analytics workload against a
// ClickHouse HTTP endpoint and a ydbgo SQL address, measuring client-side
// p50/p99 write and read latency and verifying the two systems return the same
// results. It is used to compare the ydbgo columnar CSTORE against ClickHouse
// (single node, data on /dev/shm) and against a 5-node RF=3 cluster.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ydbgo/internal/proto"
)

const (
	base      = "2024-01-01T00:00:00Z"
	tsFormat  = "2006-01-02 15:04:05.000000000"
	step      = 100 * time.Millisecond
	winStart  = "2024-01-01T03:00:00Z"
	winEnd    = "2024-01-01T04:00:00Z"
	createCH  = "CREATE TABLE logs (ts DateTime64(9, 'UTC'), level String, lat Float64) ENGINE = MergeTree ORDER BY ts"
	createYDB = "CREATE TABLE logs (ts timestamp primary key, level string, lat double) ENGINE=CSTORE"
)

var (
	levels   = []string{"info", "warn", "error"}
	levelCut = []int{60, 85, 100} // cumulative percent thresholds
)

type opts struct {
	ch      string
	ydb     string
	nodes   string
	stmts   int
	rows    int
	clients int
	reads   int
}

type latAgg struct {
	mu   sync.Mutex
	lats []time.Duration
}

func (a *latAgg) add(d time.Duration) { a.mu.Lock(); a.lats = append(a.lats, d); a.mu.Unlock() }

func (a *latAgg) pct(p float64) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.lats) == 0 {
		return 0
	}
	xs := append([]time.Duration(nil), a.lats...)
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	i := int(p * float64(len(xs)-1))
	return xs[i]
}

func (a *latAgg) n() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.lats) }

// row returns the deterministic ts/level/lat triple for row index i.
func row(i int) (ts time.Time, level string, lat float64) {
	ts = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * step)
	r := rand.New(rand.NewSource(int64(i)*2654435761 + 7))
	lat = r.Float64() * 100
	x := r.Intn(100)
	for k, c := range levelCut {
		if x < c {
			level = levels[k]
			break
		}
	}
	return ts, level, lat
}

func main() {
	o := &opts{}
	flag.StringVar(&o.ch, "ch", "http://127.0.0.1:8123", "ClickHouse HTTP endpoint (empty = skip)")
	flag.StringVar(&o.ydb, "ydb", "", "ydbgo SQL address (empty = skip)")
	flag.StringVar(&o.nodes, "nodes", "", "ClickHouse cluster node HTTP URLs, comma-separated (5); enables cluster mode")
	flag.IntVar(&o.stmts, "stmts", 960, "total write statements (shared across clients)")
	flag.IntVar(&o.rows, "rows", 200, "rows per INSERT statement")
	flag.IntVar(&o.clients, "clients", 8, "write concurrency")
	flag.IntVar(&o.reads, "reads", 100, "repetitions per read query")
	flag.Parse()

	var nodes []string
	if o.nodes != "" {
		nodes = strings.Split(o.nodes, ",")
		if len(nodes) != 5 {
			fmt.Fprintln(os.Stderr, "benchcol: -nodes expects exactly 5 URLs")
			os.Exit(2)
		}
	}

	if o.ch == "" && o.ydb == "" {
		fmt.Fprintln(os.Stderr, "benchcol: provide -ch and/or -ydb")
		os.Exit(2)
	}
	total := o.stmts * o.rows
	fmt.Printf("workload: %d statements x %d rows (concurrency %d) = %d rows\n\n", o.stmts, o.rows, o.clients, total)

	type sys struct {
		name      string
		chCluster bool
		exec      func(q string) (string, error)
	}
	var systems []*sys
	var ydbPool *proto.ConnPool

	if o.ch != "" || len(nodes) > 0 {
		if len(nodes) > 0 {
			systems = append(systems, &sys{name: "ClickHouse (5n RF3)", chCluster: true, exec: func(q string) (string, error) { return chExec(nodes[0], q, nil) }})
		} else {
			systems = append(systems, &sys{name: "ClickHouse", exec: func(q string) (string, error) { return chExec(o.ch, q, nil) }})
		}
	}
	if o.ydb != "" {
		ydbPool = proto.NewConnPool(o.clients)
		defer ydbPool.Close()
		systems = append(systems, &sys{name: "ydbgo CSTORE", exec: func(q string) (string, error) {
			var out string
			err := ydbPool.Do(o.ydb, func(c *proto.Client) error {
				r, err := c.Execute(q)
				if err != nil {
					return err
				}
				if !r.OK {
					return fmt.Errorf("%s", r.Error)
				}
				var b strings.Builder
				for _, rw := range r.Result.Rows {
					b.WriteString(strings.Join(rw, "\t"))
					b.WriteString("\n")
				}
				out = b.String()
				return nil
			})
			return out, err
		}})
	}

	// setup: drop + create on each system (cluster tables are pre-created)
	for _, s := range systems {
		if s.chCluster {
			continue
		}
		if _, err := s.exec("DROP TABLE IF EXISTS logs"); err != nil {
			fmt.Printf("[%s] setup drop: %v\n", s.name, err)
			os.Exit(1)
		}
		ddl := createCH
		if s.name == "ydbgo CSTORE" {
			ddl = createYDB
		}
		if _, err := s.exec(ddl); err != nil {
			fmt.Printf("[%s] setup create: %v\n", s.name, err)
			os.Exit(1)
		}
		if s.name == "ydbgo CSTORE" {
			// spread the table across 5 shards so all cluster nodes take part.
			for _, at := range []string{
				"'2024-01-01T01:04:00Z'", "'2024-01-01T00:32:00Z'",
				"'2024-01-01T02:08:00Z'", "'2024-01-01T03:12:00Z'",
			} {
				if _, err := s.exec("ADMIN SPLIT TABLE logs AT " + at); err != nil {
					fmt.Printf("[%s] setup split AT %s: %v\n", s.name, at, err)
					os.Exit(1)
				}
			}
		}
	}

	// write phase
	type writeRes struct {
		name string
		wall time.Duration
		agg  latAgg
	}
	var wr []*writeRes
	for _, s := range systems {
		w := &writeRes{name: s.name}
		wr = append(wr, w)
		var next int64
		var wg sync.WaitGroup
		start := time.Now()
		for c := 0; c < o.clients; c++ {
			wg.Add(1)
			go func(exec func(string) (string, error), cluster bool) {
				defer wg.Done()
				for {
					stmt := atomic.AddInt64(&next, 1) - 1
					if stmt >= int64(o.stmts) {
						return
					}
					base := int(stmt) * o.rows
					t0 := time.Now()
					var err error
					switch {
					case cluster:
						// route the whole batch to one shard (like ydbgo routes
						// rows to their owning shard); ReplicatedMergeTree plus
						// insert_quorum=2 replicates to a 2-of-3 quorum.
						shard := int(stmt) % len(nodes)
						q := fmt.Sprintf("INSERT INTO logs_%d FORMAT TabSeparated", shard)
						u := nodes[shard] + "/?query=" + url.QueryEscape(q) + "&insert_quorum=2&default_format=TabSeparated"
						err = chPost(u, chBatch(base, o.rows))
					case s.name == "ClickHouse":
						_, err = chExec(o.ch, "INSERT INTO logs FORMAT TabSeparated", chBatch(base, o.rows))
					default:
						_, err = exec(ydbBatch(base, o.rows))
					}
					if err != nil {
						fmt.Printf("[%s] write: %v\n", s.name, err)
						os.Exit(1)
					}
					w.agg.add(time.Since(t0))
				}
			}(s.exec, s.chCluster)
		}
		wg.Wait()
		w.wall = time.Since(start)
	}

	// read phase
	readQueries := func(name string, cluster bool) []string {
		table := "logs"
		if cluster {
			table = "logs_dist"
		}
		// ClickHouse cannot parse the RFC3339 'Z' suffix in DateTime64 string
		// literals, so it gets an explicit toDateTime64 cast with space-format
		// literals (same UTC semantics).
		ws, we := "'"+winStart+"'", "'"+winEnd+"'"
		if strings.HasPrefix(name, "ClickHouse") {
			ws = "toDateTime64('2024-01-01 03:00:00', 9, 'UTC')"
			we = "toDateTime64('2024-01-01 04:00:00', 9, 'UTC')"
		}
		return []string{
			"SELECT COUNT(*) FROM " + table,
			"SELECT SUM(lat), AVG(lat) FROM " + table,
			fmt.Sprintf("SELECT COUNT(*), SUM(lat) FROM %s WHERE ts >= %s AND ts < %s", table, ws, we),
			"SELECT level, COUNT(*), AVG(lat) FROM " + table + " GROUP BY level",
			"SELECT COUNT(*) FROM " + table + " WHERE level LIKE 'erro%'",
			"SELECT ts, lat FROM " + table + " ORDER BY ts DESC LIMIT 10",
		}
	}
	type readRes struct {
		sys   *sys
		qi    int
		query string
		first string
		agg   latAgg
	}
	var rr []*readRes
	for _, s := range systems {
		for qi, q := range readQueries(s.name, s.chCluster) {
			r := &readRes{sys: s, qi: qi, query: q}
			rr = append(rr, r)
			first := true
			for k := 0; k < o.reads; k++ {
				t0 := time.Now()
				out, err := s.exec(q)
				r.agg.add(time.Since(t0))
				if err != nil {
					fmt.Printf("[%s] read %q: %v\n", s.name, q, err)
					break
				}
				if first {
					r.first = strings.TrimSpace(out)
					first = false
				}
			}
		}
	}

	// report: write
	order := []string{}
	for _, w := range wr {
		order = append(order, w.name)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	byName := map[string]*writeRes{}
	for _, w := range wr {
		byName[w.name] = w
	}
	fmt.Println("== write (per-statement client-side latency) ==")
	fmt.Printf("%-14s", "system")
	for _, n := range order {
		fmt.Printf(" %18s", n)
	}
	fmt.Println()
	fmt.Printf("%-14s", "p50")
	for _, n := range order {
		fmt.Printf(" %18s", ms(byName[n].agg.pct(0.5)))
	}
	fmt.Println()
	fmt.Printf("%-14s", "p99")
	for _, n := range order {
		fmt.Printf(" %18s", ms(byName[n].agg.pct(0.99)))
	}
	fmt.Println()
	fmt.Printf("%-14s", "rows/s")
	for _, n := range order {
		fmt.Printf(" %18s", fmt.Sprintf("%.0f", float64(total)/byName[n].wall.Seconds()))
	}
	fmt.Println()

	// report: reads
	fmt.Println("\n== read (client-side latency, single query) ==")
	fmt.Printf("%-46s", "query")
	for _, n := range order {
		fmt.Printf(" %12s %12s", n+" p50", n+" p99")
	}
	fmt.Println()
	labels := []string{"count all", "sum/avg", "window", "group by level", "like prefix", "order by limit"}
	byQuery := map[int][]*readRes{}
	for _, r := range rr {
		byQuery[r.qi] = append(byQuery[r.qi], r)
	}
	for qi := 0; qi < len(readQueries(order[0], false)); qi++ {
		fmt.Printf("%-46s", labels[qi])
		for _, n := range order {
			var r *readRes
			for _, x := range byQuery[qi] {
				if x.sys.name == n {
					r = x
				}
			}
			if r == nil {
				fmt.Printf(" %12s %12s", "-", "-")
				continue
			}
			fmt.Printf(" %12s %12s", ms(r.agg.pct(0.5)), ms(r.agg.pct(0.99)))
		}
		fmt.Println()
	}

	fmt.Println("\n== result parity (first result per query) ==")
	for _, r := range rr {
		fmt.Printf("  [%s] %s\n    %s\n", r.sys.name, r.query, r.first)
	}
}

func ms(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d/time.Microsecond)
	}
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

// chPost posts an already-escaped ClickHouse HTTP URL with the given payload
// body, returning an error on non-200 responses.
func chPost(u string, body []byte) error {
	resp, err := http.Post(u, "text/tab-separated-values", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// ydbBatch builds one INSERT statement of n rows starting at global row start.
func ydbBatch(start, n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO logs VALUES ")
	for j := 0; j < n; j++ {
		ts, lv, lat := row(start + j)
		if j > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "('%s','%s',%.6f)", ts.Format(time.RFC3339Nano), lv, lat)
	}
	return b.String()
}

// chBatch builds n TabSeparated data lines starting at global row start.
func chBatch(start, n int) []byte {
	var b bytes.Buffer
	for j := 0; j < n; j++ {
		ts, lv, lat := row(start + j)
		fmt.Fprintf(&b, "%s\t%s\t%.6f\n", ts.UTC().Format(tsFormat), lv, lat)
	}
	return b.Bytes()
}

// chExec runs a ClickHouse query over HTTP. When body is non-nil it is sent as
// the request payload (INSERT data); otherwise a bare query is executed.
func chExec(addr, q string, body []byte) (string, error) {
	u := addr + "/?query=" + url.QueryEscape(q) + "&default_format=TabSeparated"
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	resp, err := http.Post(u, "text/tab-separated-values", rd)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return string(b), nil
}

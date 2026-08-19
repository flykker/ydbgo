package shard

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	sqlx "ydbgo/internal/sql"
)

// hist is a bounded reservoir of latencies used to estimate p50/p99.
type hist struct {
	mu      sync.Mutex
	samples []time.Duration
	cap     int
}

func newHist(capacity int) *hist { return &hist{cap: capacity} }

func (h *hist) add(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.samples) >= h.cap {
		// keep the reservoir truncated; copy a fresh half for stable percentiles
		half := h.samples[len(h.samples)/2:]
		h.samples = append(half, d)
		return
	}
	h.samples = append(h.samples, d)
}

func (h *hist) percentile(p float64) (time.Duration, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.samples)
	if n == 0 {
		return 0, 0
	}
	cp := make([]time.Duration, n)
	copy(cp, h.samples)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(p * float64(n))
	if idx >= n {
		idx = n - 1
	}
	return cp[idx], n
}

// metrics aggregates request latency + throughput counters on a node.
type metrics struct {
	mu       sync.Mutex
	writes   int64
	reads    int64
	writHist *hist
	readHist *hist
	// classHists tracks latency separately per query class (agg/group/order/
	// scan/point/kv/write) so ADMIN METRICS can show which statement shapes
	// dominate, without lumping every read into one bucket.
	classHist map[string]*hist
}

func newMetrics() *metrics {
	return &metrics{
		writHist: newHist(10000),
		readHist: newHist(10000),
		classHist: map[string]*hist{
			"agg":   newHist(10000),
			"group": newHist(10000),
			"order": newHist(10000),
			"scan":  newHist(10000),
			"point": newHist(10000),
			"kv":    newHist(10000),
		},
	}
}

func (m *metrics) recordRead(d time.Duration) {
	m.mu.Lock()
	m.reads++
	m.mu.Unlock()
	m.readHist.add(d)
}

func (m *metrics) recordWrite(d time.Duration) {
	m.mu.Lock()
	m.writes++
	m.mu.Unlock()
	m.writHist.add(d)
}

// recordClass adds a latency sample to the per-class histogram for a read
// statement. The class string must be one of the keys of classHist.
func (m *metrics) recordClass(class string, d time.Duration) {
	h, ok := m.classHist[class]
	if ok {
		h.add(d)
	}
}

// classOf classifies a parsed read statement by its shape, so ADMIN METRICS
// can break latency down by query kind.
func classOf(st sqlx.Statement) string {
	switch s := st.(type) {
	case *sqlx.SelectStmt:
		if len(s.GroupBy) > 0 {
			return "group"
		}
		for _, it := range s.Items {
			if it != nil && isAggStmtExpr(it.Expr) {
				return "agg"
			}
		}
		if len(s.OrderBy) > 0 {
			return "order"
		}
		if s.Where != nil {
			return "point"
		}
		return "scan"
	case *sqlx.KVGetStmt, *sqlx.KVScanStmt:
		return "kv"
	}
	return "scan"
}

// isAggStmtExpr reports whether e is (or contains) an aggregate call such as
// COUNT/SUM/MIN/MAX/AVG. Mirrors sql.isAggregate without importing internals.
func isAggStmtExpr(e sqlx.Expr) bool {
	switch n := e.(type) {
	case *sqlx.Call:
		switch n.Name {
		case "count", "sum", "min", "max", "avg", "count_if":
			return true
		}
	case *sqlx.BinaryOp:
		return isAggStmtExpr(n.Left) || isAggStmtExpr(n.Right)
	case *sqlx.CastExpr:
		return isAggStmtExpr(n.Expr)
	}
	return false
}

func (m *metrics) report() string {
	m.mu.Lock()
	w, r := m.writes, m.reads
	m.mu.Unlock()
	wp50, wn := m.writHist.percentile(0.50)
	wp99, _ := m.writHist.percentile(0.99)
	rp50, rn := m.readHist.percentile(0.50)
	rp99, _ := m.readHist.percentile(0.99)
	var classes []string
	for c := range m.classHist {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	out := fmt.Sprintf("requests: writes=%d reads=%d\n"+
		"write_latency_ms:  p50=%.3f p99=%.3f samples=%d\n"+
		"read_latency_ms:   p50=%.3f p99=%.3f samples=%d",
		w, r,
		float64(wp50)/1e6, float64(wp99)/1e6, wn,
		float64(rp50)/1e6, float64(rp99)/1e6, rn)
	for _, c := range classes {
		p50, n := m.classHist[c].percentile(0.50)
		p99, _ := m.classHist[c].percentile(0.99)
		out += fmt.Sprintf("\n%s_latency_ms:      p50=%.3f p99=%.3f samples=%d",
			c, float64(p50)/1e6, float64(p99)/1e6, n)
	}
	return out
}

// reportJSON returns the same metrics as a JSON string, for the UI backend.
func (m *metrics) reportJSON() string {
	m.mu.Lock()
	w, r := m.writes, m.reads
	m.mu.Unlock()
	wp50, wn := m.writHist.percentile(0.50)
	wp99, _ := m.writHist.percentile(0.99)
	rp50, rn := m.readHist.percentile(0.50)
	rp99, _ := m.readHist.percentile(0.99)
	rep := map[string]interface{}{
		"writes": w,
		"reads":  r,
		"write_latency_ms": map[string]interface{}{
			"p50": float64(wp50) / 1e6, "p99": float64(wp99) / 1e6, "samples": wn,
		},
		"read_latency_ms": map[string]interface{}{
			"p50": float64(rp50) / 1e6, "p99": float64(rp99) / 1e6, "samples": rn,
		},
	}
	for c, h := range m.classHist {
		p50, n := h.percentile(0.50)
		p99, _ := h.percentile(0.99)
		rep[c+"_latency_ms"] = map[string]interface{}{
			"p50": float64(p50) / 1e6, "p99": float64(p99) / 1e6, "samples": n,
		}
	}
	b, err := json.Marshal(rep)
	if err != nil {
		return "{}"
	}
	return string(b)
}

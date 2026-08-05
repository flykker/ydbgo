package shard

import (
	"fmt"
	"sort"
	"sync"
	"time"
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
}

func newMetrics() *metrics {
	return &metrics{
		writHist: newHist(10000),
		readHist: newHist(10000),
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

func (m *metrics) report() string {
	m.mu.Lock()
	w, r := m.writes, m.reads
	m.mu.Unlock()
	wp50, wn := m.writHist.percentile(0.50)
	wp99, _ := m.writHist.percentile(0.99)
	rp50, rn := m.readHist.percentile(0.50)
	rp99, _ := m.readHist.percentile(0.99)
	return fmt.Sprintf("requests: writes=%d reads=%d\n"+
		"write_latency_ms:  p50=%.3f p99=%.3f samples=%d\n"+
		"read_latency_ms:   p50=%.3f p99=%.3f samples=%d",
		w, r,
		float64(wp50)/1e6, float64(wp99)/1e6, wn,
		float64(rp50)/1e6, float64(rp99)/1e6, rn)
}

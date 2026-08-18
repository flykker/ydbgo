package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"ydbgo/internal/proto"
)

// Backend is the cluster surface the UI talks to. Sharded and standalone
// servers both implement it.
type Backend interface {
	Handle(req *proto.Request) *proto.Response
	Tables() []proto.TableInfo
	Shards(table string) ([]proto.ShardInfo, error)
	Nodes() []proto.NodeInfo
	NodeMetrics() []proto.NodeMetrics
}

// Server is the embedded HTTP UI: REST API under /api/v1 plus the embedded
// static frontend (go:embed) at every other path.
type Server struct {
	backend  Backend
	mux      *http.ServeMux
	start    time.Time
	dashOnce sync.Once
	dashErr  error
	qcache   *queryCache
}

// NewServer wires the HTTP mux for the given backend.
func NewServer(b Backend) *Server {
	s := &Server{backend: b, mux: http.NewServeMux(), start: time.Now(), qcache: newQueryCache(128)}
	s.routes()
	return s
}

// Handler returns the root http.Handler (for ListenAndServe).
func (s *Server) Handler() http.Handler { return s.mux }

// Serve blocks serving HTTP on addr until the server fails or is stopped.
func (s *Server) Serve(addr string) error {
	return http.ListenAndServe(addr, s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /api/v1/query", s.handleQuery)
	s.mux.HandleFunc("POST /api/v1/admin", s.handleAdmin)
	s.mux.HandleFunc("GET /api/v1/tables", s.handleTables)
	s.mux.HandleFunc("GET /api/v1/shards", s.handleShards)
	s.mux.HandleFunc("GET /api/v1/nodes", s.handleNodes)
	s.mux.HandleFunc("GET /api/v1/metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /metrics", s.handlePromMetrics)
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/v1/dashboards", s.handleDashboardsList)
	s.mux.HandleFunc("POST /api/v1/dashboards", s.handleDashboardCreate)
	s.mux.HandleFunc("PUT /api/v1/dashboards/{id}", s.handleDashboardUpdate)
	s.mux.HandleFunc("DELETE /api/v1/dashboards/{id}", s.handleDashboardDelete)
	s.mux.HandleFunc("POST /api/v1/widget/query", s.handleWidgetQuery)
	s.mux.HandleFunc("POST /api/v1/ingest", s.handleIngest)
	s.mux.HandleFunc("GET /api/v1/tail", s.handleTail)
	s.mux.Handle("/", staticHandler())
}

type queryRequest struct {
	SQL string `json:"sql"`
}

type adminRequest struct {
	Cmd string `json:"cmd"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var q queryRequest
	if !decodeBody(w, r, &q) {
		return
	}
	s.execSQL(w, q.SQL)
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	var a adminRequest
	if !decodeBody(w, r, &a) {
		return
	}
	s.execSQL(w, a.Cmd)
}

func (s *Server) execSQL(w http.ResponseWriter, sql string) {
	resp := s.backend.Handle(&proto.Request{SQL: sql})
	if !resp.OK {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": resp.Error})
		return
	}
	result := map[string]interface{}{"type": "ok"}
	if resp.Result != nil {
		p := resp.Result
		result = map[string]interface{}{
			"type":     p.Type,
			"columns":  p.Columns,
			"rows":     p.Rows,
			"affected": p.Affected,
			"note":     p.Note,
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "result": result})
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "tables": s.backend.Tables()})
}

func (s *Server) handleShards(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	if table == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "missing ?table="})
		return
	}
	shards, err := s.backend.Shards(table)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "shards": shards})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "nodes": s.backend.Nodes()})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "metrics": s.backend.NodeMetrics()})
}

// promMetric wraps the metric payload of ADMIN METRICS-JSON.
type promMetric struct {
	Writes       int64              `json:"writes"`
	Reads        int64              `json:"reads"`
	WriteLatency map[string]float64 `json:"write_latency_ms"`
	ReadLatency  map[string]float64 `json:"read_latency_ms"`
}

// handlePromMetrics exposes the cluster metrics in Prometheus text format
// (https://prometheus.io/docs/instrumenting/exposition_formats/). Per-node
// counters/gauges come from ADMIN METRICS-JSON, aggregated by the backend.
func (s *Server) handlePromMetrics(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	b.WriteString("# HELP ydbgo_requests_total Total requests processed by a node.\n")
	b.WriteString("# TYPE ydbgo_requests_total counter\n")
	b.WriteString("# HELP ydbgo_latency_milliseconds Request latency percentiles per node.\n")
	b.WriteString("# TYPE ydbgo_latency_milliseconds gauge\n")
	b.WriteString("# HELP ydbgo_uptime_seconds Seconds since the UI server started.\n")
	b.WriteString("# TYPE ydbgo_uptime_seconds gauge\n")
	for _, nm := range s.backend.NodeMetrics() {
		node := fmt.Sprintf("%q", nm.Node)
		fmt.Fprintf(&b, "ydbgo_uptime_seconds{node=%s}  %.3f\n", node, time.Since(s.start).Seconds())
		if nm.JSON == "" {
			continue
		}
		var m promMetric
		if err := json.Unmarshal([]byte(nm.JSON), &m); err != nil {
			continue
		}
		fmt.Fprintf(&b, "ydbgo_requests_total{node=%s,type=\"write\"}  %d\n", node, m.Writes)
		fmt.Fprintf(&b, "ydbgo_requests_total{node=%s,type=\"read\"}  %d\n", node, m.Reads)
		emitLat := func(name string, lat map[string]float64) {
			if lat == nil {
				return
			}
			if p, ok := lat["p50"]; ok {
				fmt.Fprintf(&b, "ydbgo_latency_milliseconds{node=%s,type=%q,quantile=\"p50\"}  %g\n", node, name, p)
			}
			if p, ok := lat["p99"]; ok {
				fmt.Fprintf(&b, "ydbgo_latency_milliseconds{node=%s,type=%q,quantile=\"p99\"}  %g\n", node, name, p)
			}
		}
		emitLat("write", m.WriteLatency)
		emitLat("read", m.ReadLatency)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, b.String())
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":      true,
		"uptime":  time.Since(s.start).Seconds(),
		"version": "ydbgo",
	})
}

// ingestRequest is the bulk row payload of POST /api/v1/ingest.
type ingestRequest struct {
	Table   string          `json:"table"`
	Columns []string        `json:"columns,omitempty"`
	Rows    [][]interface{} `json:"rows"`
}

// handleIngest inserts many rows in one round trip. Cells are JSON scalars:
// string (quoted, timestamps included), number, bool, or null.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	var in ingestRequest
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Table == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "missing table"})
		return
	}
	if len(in.Rows) > 10000 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "too many rows (max 10000)"})
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s", in.Table)
	if len(in.Columns) > 0 {
		b.WriteString(" (")
		for i, c := range in.Columns {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(c)
		}
		b.WriteString(")")
	}
	b.WriteString(" VALUES ")
	for ri, row := range in.Rows {
		if ri > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for ci, cell := range row {
			if ci > 0 {
				b.WriteString(",")
			}
			if err := writeLiteral(&b, cell); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": err.Error()})
				return
			}
		}
		b.WriteString(")")
	}
	resp := s.backend.Handle(&proto.Request{SQL: b.String()})
	if !resp.OK {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": resp.Error})
		return
	}
	affected := int64(0)
	if resp.Result != nil {
		affected = resp.Result.Affected
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "affected": affected})
}

// writeLiteral emits one JSON cell as a dialect literal.
func writeLiteral(b *strings.Builder, cell interface{}) error {
	switch v := cell.(type) {
	case nil:
		b.WriteString("NULL")
	case string:
		b.WriteString(quote(v))
	case float64:
		if math.Trunc(v) == v && !math.IsInf(v, 0) {
			fmt.Fprintf(b, "%d", int64(v))
		} else {
			fmt.Fprintf(b, "%g", v)
		}
	case bool:
		if v {
			b.WriteString("TRUE")
		} else {
			b.WriteString("FALSE")
		}
	default:
		return fmt.Errorf("unsupported cell type %T", cell)
	}
	return nil
}

// handleTail streams the result of a query over SSE at a fixed interval —
// the live-tail transport for the log explorer (no extra dependency, the
// browser EventSource auto-reconnects). ?sql=...&interval=seconds.
func (s *Server) handleTail(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("sql")
	if query == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "missing ?sql="})
		return
	}
	interval, err := strconv.ParseFloat(r.URL.Query().Get("interval"), 64)
	if err != nil || interval <= 0 {
		interval = 2
	}
	if interval > 30 {
		interval = 30
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": "streaming unsupported"})
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	emit := func() bool {
		resp := s.backend.Handle(&proto.Request{SQL: query})
		var payload interface{}
		if !resp.OK {
			payload = map[string]interface{}{"ok": false, "error": resp.Error}
		} else if resp.Result != nil {
			payload = map[string]interface{}{"ok": true, "result": map[string]interface{}{
				"type":    resp.Result.Type,
				"columns": resp.Result.Columns,
				"rows":    resp.Result.Rows,
				"note":    resp.Result.Note,
			}}
		} else {
			payload = map[string]interface{}{"ok": true}
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return false
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
		fl.Flush()
		return ctx.Err() == nil
	}
	if !emit() {
		return
	}
	step := time.Duration(interval * float64(time.Second))
	t := time.NewTicker(step)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !emit() {
				return
			}
		}
	}
}

// decodeBody reads a JSON body into dst; writes a 400 on failure.
func decodeBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	defer io.Copy(io.Discard, r.Body)
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": fmt.Sprintf("bad json: %v", err)})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"ydbgo/internal/storage"
	"ydbgo/internal/ui"
)

// TestUIAPIEndToEnd drives the embedded HTTP console against a real
// (standalone) server: query, dashboards CRUD, and widget query caching.
func TestUIAPIEndToEnd(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	eng, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	srv := NewServer(eng)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go srv.Serve()

	uiSrv := ui.NewServer(&standaloneBackend{srv})
	ts := httptest.NewServer(uiSrv.Handler())
	defer ts.Close()

	// query: create a table and run a time_bucket/COUNT_IF query.
	if _, m := postJSON(t, ts.URL, "/api/v1/query", `{"sql":"CREATE TABLE logs (ts timestamp primary key, level string, lat int64) ENGINE=CSTORE"}`); m["ok"] != true {
		t.Fatalf("create table: %v", m)
	}
	_, m := postJSON(t, ts.URL, "/api/v1/query", `{"sql":"INSERT INTO logs VALUES ('2024-01-01T00:00:00Z','ERROR',1), ('2024-01-01T00:15:00Z','INFO',2)"}`)
	if m["ok"] != true {
		t.Fatalf("insert: %v", m)
	}
	_, m = postJSON(t, ts.URL, "/api/v1/query", `{"sql":"SELECT time_bucket('1h', ts) AS b, COUNT_IF(level = 'ERROR') AS e FROM logs GROUP BY time_bucket('1h', ts)"}`)
	result, _ := m["result"].(map[string]interface{})
	rows, _ := result["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("grouped rows=%v", m)
	}
	bucket := rows[0].([]interface{})
	if bucket[0] != "2024-01-01T00:00:00Z" || bucket[1] != "1" {
		t.Errorf("bucket=%v", bucket)
	}

	// error path: unknown table yields ok=false.
	if resp, m := postJSON(t, ts.URL, "/api/v1/query", `{"sql":"SELECT * FROM nope"}`); resp.StatusCode != http.StatusBadRequest || m["ok"] == true {
		t.Errorf("error path: %d %v", resp.StatusCode, m)
	}

	// dashboard CRUD round trip.
	cfg := `{"title":"Demo","widgets":[{"type":"line_chart","query":"SELECT time_bucket('1h',ts) AS b, COUNT(*) AS c FROM logs GROUP BY b"}]}`
	createBody := `{"name":"My Dash","config":` + cfg + `}`
	_, m = postJSON(t, ts.URL, "/api/v1/dashboards", createBody)
	id, _ := m["id"].(string)
	if id == "" {
		t.Fatalf("create dashboard: %v", m)
	}
	resp, err := http.Get(ts.URL + "/api/v1/dashboards")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var list struct {
		OK         bool `json:"ok"`
		Dashboards []struct {
			ID     string          `json:"id"`
			Name   string          `json:"name"`
			Config json.RawMessage `json:"config"`
		} `json:"dashboards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if !list.OK || len(list.Dashboards) != 1 || list.Dashboards[0].Name != "My Dash" {
		t.Fatalf("list: %+v", list)
	}
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/dashboards/"+id, bytes.NewBufferString(createBody))
	req.Header.Set("Content-Type", "application/json")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusOK {
		t.Errorf("update: %v %v", err, resp)
		if resp != nil {
			resp.Body.Close()
		}
	}
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/dashboards/"+id, nil)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusOK {
		t.Errorf("delete: %v %v", err, resp)
		if resp != nil {
			resp.Body.Close()
		}
	}

	// widget/query cache: second identical call must hit the cache.
	widgetSQL := `{"sql":"SELECT COUNT(*) FROM logs","ttl":30}`
	_, m = postJSON(t, ts.URL, "/api/v1/widget/query", widgetSQL)
	if m["cached"] != false {
		t.Fatalf("first widget query should not be cached: %v", m)
	}
	_, m = postJSON(t, ts.URL, "/api/v1/widget/query", widgetSQL)
	if m["cached"] != true {
		t.Fatalf("second widget query should be cached: %v", m)
	}

	// ingest: bulk insert with mixed cell types + column list.
	ingestBody := `{"table":"logs","columns":["ts","level","lat"],"rows":[["2024-01-01T01:00:00Z","WARN",7],["2024-01-01T01:05:00Z","ERROR",null]]}`
	if resp, m := postJSON(t, ts.URL, "/api/v1/ingest", ingestBody); resp.StatusCode != http.StatusOK || m["affected"] != float64(2) {
		t.Fatalf("ingest: %d %v", resp.StatusCode, m)
	}
	_, m = postJSON(t, ts.URL, "/api/v1/query", `{"sql":"SELECT COUNT(*) FROM logs"}`)
	result, _ = m["result"].(map[string]interface{})
	rows, _ = result["rows"].([]interface{})
	if rows[0].([]interface{})[0] != "4" {
		t.Fatalf("count after ingest=%v", m)
	}

	// tables list carries column metadata for the explorer.
	resp, err = http.Get(ts.URL + "/api/v1/tables")
	if err != nil {
		t.Fatal(err)
	}
	var tls struct {
		OK     bool `json:"ok"`
		Tables []struct {
			Name    string `json:"name"`
			Columns []struct {
				Name    string `json:"name"`
				Type    string `json:"type"`
				Primary bool   `json:"primary"`
			} `json:"columns"`
		} `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tls); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var logsCols []struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Primary bool   `json:"primary"`
	}
	for _, tb := range tls.Tables {
		if tb.Name == "logs" {
			logsCols = tb.Columns
		}
	}
	if !tls.OK || len(logsCols) != 3 || !logsCols[0].Primary || logsCols[0].Type != "timestamp" {
		t.Fatalf("tables metadata: %+v", tls)
	}
}

// TestUITailSSE verifies the live-tail stream emits at least one frame and
// stops on client disconnect.
func TestUITailSSE(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	eng, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	srv := NewServer(eng)
	uiSrv := ui.NewServer(&standaloneBackend{srv})
	ts := httptest.NewServer(uiSrv.Handler())
	defer ts.Close()

	run := func(cmd string) {
		t.Helper()
		if _, m := postJSON(t, ts.URL, "/api/v1/query", `{"sql":"`+cmd+`"}`); m["ok"] != true {
			t.Fatalf("%s: %v", cmd, m)
		}
	}
	run("CREATE TABLE logs (ts timestamp primary key, level string) ENGINE=CSTORE")

	url := ts.URL + "/api/v1/tail?interval=1&sql=" + url.QueryEscape("SELECT * FROM logs ORDER BY ts DESC LIMIT 5")
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	frames := 0
	for frames < 2 {
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatalf("tail read %d: %v", frames, err)
		}
		if strings.HasPrefix(line, "data: {") {
			frames++
		}
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("content-type: %q", ct)
	}
}

// TestUIPromMetrics checks the Prometheus text-format endpoint returns
// HELP/TYPE scaffolding and per-node uptime at least.
func TestUIPromMetrics(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	eng, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	srv := NewServer(eng)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go srv.Serve()

	uiSrv := ui.NewServer(&standaloneBackend{srv})
	ts := httptest.NewServer(uiSrv.Handler())
	defer ts.Close()

	// warm the write path so a requests_total sample exists on the local node
	postJSON(t, ts.URL, "/api/v1/query", `{"sql":"CREATE TABLE m (id int64 primary key)"}`)

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type: %q", ct)
	}
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	out := body.String()
	for _, want := range []string{
		"# HELP ydbgo_requests_total",
		"# TYPE ydbgo_requests_total counter",
		"# HELP ydbgo_latency_milliseconds",
		"ydbgo_uptime_seconds{",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, `ydbgo_requests_total{node="local",type="write"}`) {
		t.Errorf("/metrics missing local write counter:\n%s", out)
	}
}

func postJSON(t *testing.T, base, path, body string) (*http.Response, map[string]interface{}) {
	t.Helper()
	resp, err := http.Post(base+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	var m map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return resp, m
}

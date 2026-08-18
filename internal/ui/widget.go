package ui

import (
	"net/http"
	"sync"
	"time"

	"ydbgo/internal/proto"
)

// queryCache is a small LRU with TTL for dashboard widget queries. The same SQL
// on a dashboard refreshes at most once per TTL; the widget query is cheap to
// re-run but caches smooth out rapid panel re-renders.
type queryCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]*qentry
	order   []string
}

type qentry struct {
	sql     string
	expires time.Time
	result  map[string]interface{}
}

func newQueryCache(max int) *queryCache {
	return &queryCache{max: max, entries: map[string]*qentry{}}
}

func (c *queryCache) get(sql string, ttl time.Duration) (map[string]interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sql]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	// move to front of LRU order
	for i, k := range c.order {
		if k == sql {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.order = append(c.order, sql)
	return e.result, true
}

func (c *queryCache) put(sql string, ttl time.Duration, result map[string]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[sql]; !ok {
		c.order = append(c.order, sql)
	}
	c.entries[sql] = &qentry{sql: sql, expires: time.Now().Add(ttl), result: result}
	for len(c.order) > c.max {
		lru := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, lru)
	}
}

type widgetQueryRequest struct {
	SQL string `json:"sql"`
	TTL int    `json:"ttl"` // cache lifetime seconds; 0 = no cache
}

func (s *Server) handleWidgetQuery(w http.ResponseWriter, r *http.Request) {
	var q widgetQueryRequest
	if !decodeBody(w, r, &q) {
		return
	}
	if q.SQL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "sql required"})
		return
	}
	ttl := time.Duration(q.TTL) * time.Second
	if ttl > 0 {
		if cached, ok := s.qcache.get(q.SQL, ttl); ok {
			writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "result": cached, "cached": true})
			return
		}
	}
	resp := s.backend.Handle(&proto.Request{SQL: q.SQL})
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
		}
	}
	if ttl > 0 {
		s.qcache.put(q.SQL, ttl, result)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "result": result, "cached": false})
}

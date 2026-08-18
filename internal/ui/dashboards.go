package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"ydbgo/internal/proto"
)

// dashboardsTable is a normal replicated TABLE storing dashboard configs.
const dashboardsTable = "_dashboards"

var dashboardsDDL = "CREATE TABLE " + dashboardsTable +
	" (id string primary key, name string, config string, updated string) ENGINE=TABLE"

// ensureDashboards runs the dashboards DDL once. create_table is idempotent in
// the meta FSM, so a repeat is a no-op; we still guard with sync.Once.
func (s *Server) ensureDashboards() {
	s.dashOnce.Do(func() {
		resp := s.backend.Handle(&proto.Request{SQL: dashboardsDDL})
		if resp != nil && !resp.OK {
			s.dashErr = fmt.Errorf("ensure dashboards table: %s", resp.Error)
		}
	})
}

type dashboardRow struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Config  json.RawMessage `json:"config"`
	Updated string          `json:"updated"`
}

type dashboardRequest struct {
	Name   string          `json:"name"`
	Config json.RawMessage `json:"config"`
}

func (s *Server) handleDashboardsList(w http.ResponseWriter, r *http.Request) {
	s.ensureDashboards()
	if s.dashErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": s.dashErr.Error()})
		return
	}
	resp := s.backend.Handle(&proto.Request{SQL: "SELECT id, name, config, updated FROM " + dashboardsTable})
	if !resp.OK {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": resp.Error})
		return
	}
	rows := make([]dashboardRow, 0)
	if resp.Result != nil {
		for _, rw := range resp.Result.Rows {
			rows = append(rows, dashboardRow{
				ID:      at(rw, 0),
				Name:    at(rw, 1),
				Config:  json.RawMessage(at(rw, 2)),
				Updated: at(rw, 3),
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "dashboards": rows})
}

func (s *Server) handleDashboardCreate(w http.ResponseWriter, r *http.Request) {
	var d dashboardRequest
	if !decodeBody(w, r, &d) {
		return
	}
	if d.Name == "" || len(d.Config) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "name and config required"})
		return
	}
	id := fmt.Sprintf("d%d", time.Now().UnixNano())
	s.ensureDashboards()
	if s.dashErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": s.dashErr.Error()})
		return
	}
	if err := s.upsertDashboard(id, d.Name, d.Config); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true, "id": id})
}

func (s *Server) handleDashboardUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var d dashboardRequest
	if !decodeBody(w, r, &d) {
		return
	}
	if d.Name == "" || len(d.Config) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "name and config required"})
		return
	}
	s.ensureDashboards()
	if s.dashErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": s.dashErr.Error()})
		return
	}
	if err := s.upsertDashboard(id, d.Name, d.Config); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) handleDashboardDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp := s.backend.Handle(&proto.Request{SQL: "DELETE FROM " + dashboardsTable + " WHERE id = " + quote(id)})
	if !resp.OK {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": resp.Error})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (s *Server) upsertDashboard(id, name string, config json.RawMessage) error {
	sql := "INSERT INTO " + dashboardsTable + " VALUES (" + quote(id) + ", " + quote(name) + ", " +
		quote(string(config)) + ", " + quote(time.Now().UTC().Format(time.RFC3339)) + ")"
	resp := s.backend.Handle(&proto.Request{SQL: sql})
	if resp != nil && !resp.OK {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

func at(row []string, i int) string {
	if i < len(row) {
		return row[i]
	}
	return ""
}

// quote wraps s as a SQL string literal: single-quoted with embedded single
// quotes doubled (the dialect has no backslash escapes).
func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

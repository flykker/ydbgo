package shard

import (
	"fmt"
	"time"

	sqlx "ydbgo/internal/sql"
)

// ttlLoop periodically purges rows older than each table's RETENTION window.
// The delete is proposed through the shard's Raft group on the shard leader, so
// every replica applies the same DeleteRange; non-leaders skip the work.
func (m *Manager) ttlLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.ttlTick)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.runRetention()
		}
	}
}

func (m *Manager) runRetention() {
	cat := m.meta.FSM().Catalog()
	for _, ts := range cat.Tables {
		if ts.Schema == nil || ts.Schema.Retention <= 0 {
			continue
		}
		pkCol, ok := retentionPkColumn(ts.Schema)
		if !ok {
			continue
		}
		cutoff := time.Now().UTC().Add(-ts.Schema.Retention)
		cutoffStr := cutoff.Format(time.RFC3339Nano)
		for _, spec := range ts.Shards {
			if !m.hosts(spec) {
				continue
			}
			sh := m.localShard(spec.ID)
			if sh == nil || sh.frozen || !sh.node.IsLeader() {
				continue
			}
			cnt, err := sh.node.Execute(fmt.Sprintf(
				"SELECT COUNT(*) AS c FROM %s WHERE %s < '%s'", ts.Schema.Name, pkCol, cutoffStr))
			if err != nil || cnt == nil || len(cnt.Rows) == 0 || cnt.Rows[0][0].Int <= 0 {
				continue
			}
			_, _ = sh.node.Execute(fmt.Sprintf(
				"DELETE FROM %s WHERE %s < '%s'", ts.Schema.Name, pkCol, cutoffStr))
		}
	}
}

// retentionPkColumn returns the table's PK column name when it is the single
// timestamp column a time-based retention window can be applied to.
func retentionPkColumn(s *sqlx.TableSchema) (string, bool) {
	if len(s.PK) != 1 {
		return "", false
	}
	for _, c := range s.Columns {
		if c.Name == s.PK[0] {
			return c.Name, c.Type == sqlx.TypeTimestamp
		}
	}
	return "", false
}

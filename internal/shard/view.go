package shard

import (
	"sort"

	"ydbgo/internal/proto"
)

// Tables returns a cluster-wide summary of every table in the catalog.
func (m *Manager) Tables() []proto.TableInfo {
	cat := m.meta.FSM().Catalog()
	out := make([]proto.TableInfo, 0, len(cat.Tables))
	for name, ts := range cat.Tables {
		var size uint64
		for _, s := range ts.Shards {
			size += s.Size
		}
		ti := proto.TableInfo{Name: name, Engine: ts.Schema.Engine, Shards: len(ts.Shards), Size: size}
		if ts.Schema.Retention > 0 {
			ti.Retention = ts.Schema.Retention.String()
		}
		for _, c := range ts.Schema.Columns {
			primary := c.AsPrimary
			for _, pk := range ts.Schema.PK {
				if pk == c.Name {
					primary = true
				}
			}
			ti.Columns = append(ti.Columns, proto.ColumnInfo{Name: c.Name, Type: c.Type.String(), Primary: primary})
		}
		out = append(out, ti)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Shards lists the shards of one table.
func (m *Manager) Shards(table string) ([]proto.ShardInfo, error) {
	ts := m.table(table)
	if ts == nil {
		return nil, notFound(table)
	}
	out := make([]proto.ShardInfo, 0, len(ts.Shards))
	for _, s := range ts.Shards {
		out = append(out, proto.ShardInfo{
			ID:    s.ID,
			Start: string(s.Start),
			End:   string(s.End),
			Nodes: s.Nodes,
			Size:  s.Size,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Nodes lists all registered cluster nodes from the catalog.
func (m *Manager) Nodes() []proto.NodeInfo {
	cat := m.meta.FSM().Catalog()
	out := make([]proto.NodeInfo, 0, len(cat.Specs))
	for _, spec := range cat.Specs {
		out = append(out, proto.NodeInfo{ID: spec.ID, SQLAddr: spec.SQLAddr, RaftAddr: spec.RaftAddr})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// NodeMetrics aggregates per-node ADMIN METRICS-JSON across the cluster.
func (m *Manager) NodeMetrics() []proto.NodeMetrics {
	cat := m.meta.FSM().Catalog()
	out := make([]proto.NodeMetrics, 0, len(cat.Specs))
	for _, spec := range cat.Specs {
		nm := proto.NodeMetrics{Node: spec.ID, Addr: spec.SQLAddr, Status: "up"}
		if spec.ID == m.id {
			nm.JSON = m.met.reportJSON()
		} else if r, err := m.remoteAdmin(spec.SQLAddr, "ADMIN METRICS-JSON"); err != nil || r == nil || !r.OK {
			nm.Status = "down"
		} else if r.Result != nil {
			nm.JSON = r.Result.Note
		}
		out = append(out, nm)
	}
	return out
}

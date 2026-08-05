package shard

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	sqlx "ydbgo/internal/sql"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitMetaLeader(t *testing.T, m *MetaNode, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if m.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no meta leader")
}

func waitCount(t *testing.T, m *MetaNode, tables int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if len(m.FSM().Catalog().Tables) == tables {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("catalog tables=%d want %d", len(m.FSM().Catalog().Tables), tables)
}

func schema(name string) *sqlx.TableSchema {
	return &sqlx.TableSchema{
		Name: name,
		Columns: []sqlx.ColumnDef{
			{Name: "id", Type: sqlx.TypeInt, AsPrimary: true, NotNull: true},
			{Name: "v", Type: sqlx.TypeString},
		},
		PK: []string{"id"},
	}
}

func TestMetaSingleNodeCatalog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "meta")
	m := NewMetaNode("n1", freePort(t), dir)
	defer m.Close()
	if err := m.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitMetaLeader(t, m, 5*time.Second)

	if err := m.RegisterNode("n1", "127.0.0.1:2135", "127.0.0.1:7001"); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateTable(schema("users"), 1); err != nil {
		t.Fatal(err)
	}
	cat := m.FSM().Catalog()
	ts, ok := cat.Tables["users"]
	if !ok {
		t.Fatal("users table missing")
	}
	if len(ts.Shards) != 1 {
		t.Fatalf("shards=%d", len(ts.Shards))
	}
	if ts.Shards[0].ID != "users-0" || len(ts.Shards[0].Nodes) != 1 {
		t.Fatalf("shard=%+v", ts.Shards[0])
	}

	// persistence across restart
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2 := NewMetaNode("n1", freePort(t), dir)
	defer m2.Close()
	if err := m2.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitMetaLeader(t, m2, 5*time.Second)
	cat2 := m2.FSM().Catalog()
	if _, ok := cat2.Tables["users"]; !ok {
		t.Fatal("catalog lost after restart")
	}
}

func TestMetaLateJoinerCatchUp(t *testing.T) {
	dirA := filepath.Join(t.TempDir(), "a")
	mA := NewMetaNode("a", freePort(t), dirA)
	defer mA.Close()
	if err := mA.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitMetaLeader(t, mA, 5*time.Second)
	if err := mA.RegisterNode("a", "127.0.0.1:2135", "127.0.0.1:7001"); err != nil {
		t.Fatal(err)
	}
	if err := mA.CreateTable(schema("users"), 1); err != nil {
		t.Fatal(err)
	}

	// a brand-new node joins after the catalog has entries: must catch up
	// via raft snapshot (file stores), not just empty log.
	dirB := filepath.Join(t.TempDir(), "b")
	mB := NewMetaNode("b", freePort(t), dirB)
	defer mB.Close()
	if err := mB.Start(false, nil); err != nil {
		t.Fatal(err)
	}
	if err := mA.Group().AddPeer("b", mB.Group().Advertise()); err != nil {
		t.Fatal(err)
	}
	waitCount(t, mB, 1, 10*time.Second)
	if err := mA.RegisterNode("b", "127.0.0.1:2136", "127.0.0.1:7002"); err != nil {
		t.Fatal(err)
	}
	// wait until B sees the registered node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := mB.FSM().Catalog().Specs["b"]; ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("late joiner did not catch up catalog")
}

func TestMetaPlacementRF(t *testing.T) {
	ids := []string{"a", "b", "c"}
	dirs := []string{t.TempDir() + "/a", t.TempDir() + "/b", t.TempDir() + "/c"}
	meta := make([]*MetaNode, 3)
	for i, id := range ids {
		meta[i] = NewMetaNode(id, freePort(t), dirs[i])
		defer meta[i].Close()
	}
	for i, m := range meta {
		if err := m.Start(i == 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	// form the meta cluster
	var leader *MetaNode
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range meta {
			if m.IsLeader() {
				leader = m
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	for i, m := range meta {
		if m == leader {
			continue
		}
		if err := leader.Group().AddPeer(ids[i], m.Group().Advertise()); err != nil {
			t.Fatalf("add peer %s: %v", ids[i], err)
		}
	}
	// register all nodes via the leader
	for _, id := range ids {
		if err := leader.RegisterNode(id, freePort(t), freePort(t)); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	// wait until the leader sees all 3 registered
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if leader.IsLeader() && len(leader.FSM().Catalog().Nodes) == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(leader.FSM().Catalog().Nodes) != 3 {
		t.Fatalf("registered nodes=%d", len(leader.FSM().Catalog().Nodes))
	}
	if err := leader.CreateTable(schema("users"), 2); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	var sh *ShardSpec
	for time.Now().Before(deadline) {
		ts, ok := leader.FSM().Catalog().Tables["users"]
		if ok && len(ts.Shards) == 1 {
			sh = ts.Shards[0]
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if sh == nil {
		t.Fatal("no shard")
	}
	if len(sh.Nodes) != 2 {
		t.Errorf("rf=2 but shard on %v", sh.Nodes)
	}
}

package raftsvc

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
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

func TestSingleNodeCluster(t *testing.T) {
	n, err := NewNode(Config{
		ID:       "n1",
		RaftAddr: freePort(t),
		DataDir:  filepath.Join(t.TempDir(), "data"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, n, 5*time.Second)

	if _, err := n.Execute("CREATE TABLE t (id int64 primary key, v string)"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')"); err != nil {
		t.Fatal(err)
	}
	r, err := n.Execute("SELECT v FROM t WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Str != "b" {
		t.Errorf("rows=%v", r.Rows)
	}
	// persistence across restart
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n2, err := NewNode(Config{
		ID:       "n1",
		RaftAddr: freePort(t),
		DataDir:  n.dataDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if err := n2.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, n2, 5*time.Second)
	r, err = n2.Execute("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Int != 2 {
		t.Errorf("count=%v", r.Rows[0][0])
	}
}

// TestKVRawReplication exercises the raw byte-KV surface end to end through
// raft: CREATE TABLE ... ENGINE=KV, then KV PUT/GET/DELETE/SCAN as raft entries,
// plus persistence across restart (the FSM snapshot roundtrip includes the raw
// KV area).
func TestKVRawReplication(t *testing.T) {
	n, err := NewNode(Config{
		ID:       "n1",
		RaftAddr: freePort(t),
		DataDir:  filepath.Join(t.TempDir(), "data"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, n, 5*time.Second)

	if _, err := n.Execute("CREATE TABLE kv_t (id int64 primary key) ENGINE=KV"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Execute("KV PUT kv_t 'alpha' '1'"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Execute("KV PUT kv_t 'beta' '2'"); err != nil {
		t.Fatal(err)
	}
	r, err := n.Execute("KV GET kv_t 'beta'")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][1].Str != "2" {
		t.Errorf("kv get rows=%v", r.Rows)
	}
	if _, err := n.Execute("KV DELETE kv_t 'alpha'"); err != nil {
		t.Fatal(err)
	}
	r, err = n.Execute("KV SCAN kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Str != "beta" || r.Rows[0][1].Str != "2" {
		t.Errorf("kv scan rows=%v", r.Rows)
	}
	// raw KV on a non-KV table must fail
	if _, err := n.Execute("CREATE TABLE row_t (id int64 primary key)"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Execute("KV PUT row_t 'k' 'v'"); err == nil {
		t.Fatal("KV PUT on TABLE engine should fail")
	}
	// persistence across restart (snapshot includes raw KV area)
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n2, err := NewNode(Config{
		ID:       "n1",
		RaftAddr: freePort(t),
		DataDir:  n.dataDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if err := n2.Start(true, nil); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, n2, 5*time.Second)
	r, err = n2.Execute("KV SCAN kv_t")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Str != "beta" {
		t.Errorf("kv scan after restart=%v", r.Rows)
	}
}

func TestThreeNodeCluster(t *testing.T) {
	base := t.TempDir()
	addrs := []string{freePort(t), freePort(t), freePort(t)}
	servers := []raft.Server{
		{ID: "a", Address: raft.ServerAddress(addrs[0])},
		{ID: "b", Address: raft.ServerAddress(addrs[1])},
		{ID: "c", Address: raft.ServerAddress(addrs[2])},
	}
	nodes := make([]*Node, 3)
	for i, s := range servers {
		n, err := NewNode(Config{
			ID:       string(s.ID),
			RaftAddr: addrs[i],
			DataDir:  filepath.Join(base, string(s.ID)),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
	}
	// start all; only the first bootstraps, others join as followers
	for i, n := range nodes {
		if err := n.Start(i == 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	// wait for a leader
	deadline := time.Now().Add(8 * time.Second)
	var leader *Node
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// add the two followers as voters
	for _, n := range nodes {
		if n == leader {
			continue
		}
		if err := leader.AddPeer(string(n.id), n.raftAddr); err != nil {
			t.Fatalf("AddPeer %s: %v", n.id, err)
		}
	}

	// wait until configuration has 3 voters
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		cfgs := leader.Peers()
		if len(cfgs) == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := len(leader.Peers()); got != 3 {
		t.Fatalf("peers=%d, want 3", got)
	}

	if _, err := leader.Execute("CREATE TABLE t (id int64 primary key, v string)"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Execute("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')"); err != nil {
		t.Fatal(err)
	}

	// reads on all replicas converge
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		allOK := true
		for _, n := range nodes {
			r, err := n.Execute("SELECT COUNT(*) FROM t")
			if err != nil || r == nil || len(r.Rows) == 0 || r.Rows[0][0].Int != 3 {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("replicas did not converge")
}

func waitLeader(t *testing.T, n *Node, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader")
}

func TestLeaderFailover(t *testing.T) {
	base := t.TempDir()
	addrs := []string{freePort(t), freePort(t), freePort(t)}
	servers := []raft.Server{
		{ID: "a", Address: raft.ServerAddress(addrs[0])},
		{ID: "b", Address: raft.ServerAddress(addrs[1])},
		{ID: "c", Address: raft.ServerAddress(addrs[2])},
	}
	nodes := make([]*Node, 3)
	for i, s := range servers {
		n, err := NewNode(Config{
			ID:       string(s.ID),
			RaftAddr: addrs[i],
			DataDir:  filepath.Join(base, string(s.ID)),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
	}
	for i, n := range nodes {
		if err := n.Start(i == 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()

	// form 3-node cluster
	var leader *Node
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}
	for _, n := range nodes {
		if n == leader {
			continue
		}
		if err := leader.AddPeer(string(n.id), n.raftAddr); err != nil {
			t.Fatalf("AddPeer: %v", err)
		}
	}
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && len(leader.Peers()) != 3 {
		time.Sleep(100 * time.Millisecond)
	}
	if len(leader.Peers()) != 3 {
		t.Fatalf("peers=%d", len(leader.Peers()))
	}

	// write before failover
	if _, err := leader.Execute("CREATE TABLE t (id int64 primary key, v string)"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')"); err != nil {
		t.Fatal(err)
	}

	// kill the leader
	leaderID := leader.id
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}

	// wait for new leader among survivors
	var newLeader *Node
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.id == leaderID || n.group.raft == nil {
				continue
			}
			if n.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader after failover")
	}

	// write on the new leader and read back
	if _, err := newLeader.Execute("INSERT INTO t VALUES (3, 'three')"); err != nil {
		t.Fatal(err)
	}
	r, err := newLeader.Execute("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Int != 3 {
		t.Errorf("count=%v, want 3", r.Rows[0][0])
	}
}

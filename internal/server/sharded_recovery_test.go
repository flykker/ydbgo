package server

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestShardedFiveNodeRecovery kills a node in a 5-node RF=3 cluster and
// verifies the meta leader's recovery loop heals every shard back to RF on
// the surviving nodes: dead members leave the placement, replacements catch
// up, and reads/writes keep working throughout.
func TestShardedFiveNodeRecovery(t *testing.T) {
	const rf = 3
	const recTick = 150 * time.Millisecond
	n1 := startClusterNodeR(t, "n1", freePort(t), "", rf, recTick)
	defer n1.stop()
	nodes := []*shardNode{n1}
	for i := 2; i <= 5; i++ {
		id := fmt.Sprintf("n%d", i)
		n := startClusterNodeR(t, id, freePort(t), n1.sql1, rf, recTick)
		defer n.stop()
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		waitRegistered(t, n, 5, 15*time.Second)
	}

	execOK(t, n1.c, "CREATE TABLE users (id int64 primary key, name string)")
	execOK(t, n1.c, "CREATE TABLE orders (id int64 primary key, amount int64)")
	for _, at := range []string{"100", "200"} {
		execOK(t, n1.c, fmt.Sprintf("ADMIN SPLIT TABLE users AT %s", at))
		execOK(t, n1.c, fmt.Sprintf("ADMIN SPLIT TABLE orders AT %s", at))
	}
	time.Sleep(500 * time.Millisecond)

	for i := 1; i <= 300; i++ {
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d)", i, i*10))
	}
	for _, n := range nodes {
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM users", "300", 20*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM orders", "300", 20*time.Second)
	}

	// kill n3; it stays registered in the catalog, so the recovery leader
	// must detect it as dead and heal around it
	deadID := "n3"
	var dead *shardNode
	for _, n := range nodes {
		if n.id == deadID {
			dead = n
		}
	}
	dead.stop()

	// writes must keep working on live nodes right after the failure
	execOK(t, n1.c, "INSERT INTO users VALUES (501, 'post-fail')")

	// wait until every shard of every table shows rf placement, all alive
	deadline := time.Now().Add(90 * time.Second)
	for {
		healed := true
		for _, tbl := range []string{"users", "orders"} {
			sh := execOK(t, n1.c, "ADMIN SHARDS "+tbl)
			for _, row := range sh.Result.Rows {
				nl := strings.Split(row[3], ",")
				if len(nl) != rf {
					healed = false
				}
				for _, id := range nl {
					if id == deadID {
						healed = false
					}
				}
			}
		}
		if healed {
			break
		}
		if time.Now().After(deadline) {
			sh := execOK(t, n1.c, "ADMIN SHARDS users")
			t.Fatalf("shards never healed: %v", sh.Result.Rows)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// full dataset visible on every surviving node, replacement included
	for _, n := range nodes {
		if n.id == deadID {
			continue
		}
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM users", "301", 30*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM orders", "300", 30*time.Second)
	}

	// writes after healing replicate to the fresh replica
	execOK(t, n1.c, "INSERT INTO orders VALUES (999, 9999)")
	for _, n := range nodes {
		if n.id == deadID {
			continue
		}
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM orders", "301", 30*time.Second)
	}

	// point reads post-recovery
	r := execOK(t, n1.c, "SELECT name FROM users WHERE id = 501")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "post-fail" {
		t.Fatalf("point read after recovery: %v", r.Result.Rows)
	}
	r = execOK(t, n1.c, "SELECT amount FROM orders WHERE id = 999")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "9999" {
		t.Fatalf("point read after recovery: %v", r.Result.Rows)
	}
}

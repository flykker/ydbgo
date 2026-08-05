package server

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// waitRegistered polls until every node in the catalog is registered and
// visible from this node (join + meta replication complete).
func waitRegistered(t *testing.T, n *shardNode, want int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		cat := n.mgr.Meta().FSM().Catalog()
		if cat != nil && len(cat.Nodes) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("catalog on %s never reached %d nodes (got %d)", n.id, want, len(n.mgr.Meta().FSM().Catalog().Nodes))
}

// TestShardedFiveNodeCluster runs a 5-node RF=3 cluster with several tables,
// several shards per table, and verifies every shard reads and writes from
// every node.
func TestShardedFiveNodeCluster(t *testing.T) {
	const rf = 3
	n1 := startClusterNode(t, "n1", freePort(t), "", rf)
	defer n1.stop()
	nodes := []*shardNode{n1}
	for i := 2; i <= 5; i++ {
		id := fmt.Sprintf("n%d", i)
		n := startClusterNode(t, id, freePort(t), n1.sql1, rf)
		defer n.stop()
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		waitRegistered(t, n, 5, 15*time.Second)
	}

	// three tables with different PK types
	execOK(t, n1.c, "CREATE TABLE users (id int64 primary key, name string)")
	execOK(t, n1.c, "CREATE TABLE orders (id int64 primary key, amount int64)")
	execOK(t, n1.c, "CREATE TABLE products (sku string primary key, qty int64)")

	// split each table into several shards (manual splits on the meta leader)
	for _, at := range []string{"100", "200", "300"} {
		execOK(t, n1.c, fmt.Sprintf("ADMIN SPLIT TABLE users AT %s", at))
		execOK(t, n1.c, fmt.Sprintf("ADMIN SPLIT TABLE orders AT %s", at))
		execOK(t, n1.c, fmt.Sprintf("ADMIN SPLIT TABLE products AT 'sku%s'", at))
	}
	time.Sleep(500 * time.Millisecond)

	// each table now has 4 shards, each placed on exactly rf nodes
	for _, tbl := range []string{"users", "orders", "products"} {
		sh := execOK(t, n1.c, "ADMIN SHARDS "+tbl)
		if len(sh.Result.Rows) != 4 {
			t.Fatalf("%s: expected 4 shards, got %v", tbl, sh.Result.Rows)
		}
		for _, row := range sh.Result.Rows {
			nl := strings.Split(row[3], ",")
			if len(nl) != rf {
				t.Fatalf("%s shard %s placed on %d nodes, want %d: %v", tbl, row[0], len(nl), rf, row[3])
			}
		}
	}

	// seed data across all shards of all tables
	for i := 1; i <= 400; i++ {
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d)", i, i*10))
		execOK(t, n1.c, fmt.Sprintf("INSERT INTO products VALUES ('sku%d', %d)", i, i))
	}

	// every node must see the full dataset for every table
	for _, n := range nodes {
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM users", "400", 20*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM orders", "400", 20*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM products", "400", 20*time.Second)
	}

	// point reads by PK from every node on every table
	for _, n := range nodes {
		r := execOK(t, n.c, "SELECT name FROM users WHERE id = 250")
		if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "user250" {
			t.Fatalf("%s users point read: %v", n.id, r.Result.Rows)
		}
		r = execOK(t, n.c, "SELECT amount FROM orders WHERE id = 7")
		if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "70" {
			t.Fatalf("%s orders point read: %v", n.id, r.Result.Rows)
		}
		r = execOK(t, n.c, "SELECT qty FROM products WHERE sku = 'sku333'")
		if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "333" {
			t.Fatalf("%s products point read: %v", n.id, r.Result.Rows)
		}
	}

	// writes through EVERY node (not just the meta leader) must be visible
	// everywhere afterwards
	for i, n := range nodes {
		base := 1000 + i*100
		execOK(t, n.c, fmt.Sprintf("INSERT INTO users VALUES (%d, 'extra%d')", base, i))
		execOK(t, n.c, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d)", base, i))
		execOK(t, n.c, fmt.Sprintf("INSERT INTO products VALUES ('sku%d', %d)", base, i))
	}
	for _, n := range nodes {
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM users", "405", 20*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM orders", "405", 20*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM products", "405", 20*time.Second)
	}

	// updates and deletes on the sharded tables via the meta leader
	execOK(t, n1.c, "UPDATE users SET name = 'renamed' WHERE id = 100")
	execOK(t, n1.c, "DELETE FROM orders WHERE id = 101")
	execOK(t, n1.c, "UPDATE products SET qty = 999 WHERE sku = 'sku100'")
	for _, n := range nodes {
		waitQuery(t, n.c, "SELECT name FROM users WHERE id = 100", "renamed", 20*time.Second)
		waitQuery(t, n.c, "SELECT COUNT(*) AS c FROM orders", "404", 20*time.Second)
		waitQuery(t, n.c, "SELECT qty FROM products WHERE sku = 'sku100'", "999", 20*time.Second)
	}
}

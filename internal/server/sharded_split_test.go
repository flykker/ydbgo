package server

import (
	"testing"
	"time"

	"ydbgo/internal/proto"
)

// TestShardedCompositePKSplit verifies ADMIN SPLIT on a composite (multi-column)
// primary key: the AT spec accepts a parenthesized list, the split boundary is
// type-correct, rows stay reachable across the boundary and new writes land on
// the fresh shard. Wrong arity is rejected.
func TestShardedCompositePKSplit(t *testing.T) {
	n1 := startClusterNode(t, "n1", freePort(t), "", 2)
	defer n1.stop()
	n2 := startClusterNode(t, "n2", freePort(t), n1.sql1, 2)
	defer n2.stop()

	execOK(t, n1.c, "CREATE TABLE events (user_id int64, ts timestamp, v string, PRIMARY KEY (user_id, ts))")
	execOK(t, n1.c, "INSERT INTO events VALUES (1, '2024-01-01T00:00:00Z', 'a'), (1, '2024-06-01T00:00:00Z', 'b'), (2, '2024-01-01T00:00:00Z', 'c')")

	// split at composite boundary (user_id, ts) = (1, 2024-06-01)
	execOK(t, n1.c, "ADMIN SPLIT TABLE events AT (1, '2024-06-01T00:00:00Z')")
	sh := execOK(t, n1.c, "ADMIN SHARDS events")
	if len(sh.Result.Rows) != 2 {
		t.Fatalf("after composite split: %v", sh.Result.Rows)
	}
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM events", "3", 15*time.Second)

	// rows on both sides of the boundary remain point-readable
	r := execOK(t, n1.c, "SELECT v FROM events WHERE user_id = 2")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "c" {
		t.Fatalf("select user 2: %v", r.Result.Rows)
	}
	r = execOK(t, n2.c, "SELECT v FROM events WHERE user_id = 1 AND ts = '2024-01-01T00:00:00Z'")
	if len(r.Result.Rows) != 1 || r.Result.Rows[0][0] != "a" {
		t.Fatalf("select user 1 ts jan: %v", r.Result.Rows)
	}

	// new writes (beyond the boundary) still visible from both nodes
	execOK(t, n2.c, "INSERT INTO events VALUES (2, '2024-09-01T00:00:00Z', 'd')")
	waitQuery(t, n1.c, "SELECT COUNT(*) AS c FROM events", "4", 15*time.Second)

	// wrong number of values must be rejected
	for _, bad := range []string{
		"ADMIN SPLIT TABLE events AT 1",
		"ADMIN SPLIT TABLE events AT (1, '2024-06-01T00:00:00Z', 9)",
		"ADMIN SPLIT TABLE events AT ()",
	} {
		r := execRaw(t, n1.c, bad)
		if r.OK {
			t.Fatalf("%q: split with wrong arity must fail", bad)
		}
	}

	// a value whose type does not match the column is rejected
	if r := execRaw(t, n1.c, "ADMIN SPLIT TABLE events AT ('x', '2024-06-01T00:00:00Z')"); r.OK {
		t.Fatalf("bad type must fail")
	}
}

// execRaw executes without failing the test on an error response.
func execRaw(t *testing.T, c *proto.Client, sql string) *proto.Response {
	t.Helper()
	r, err := c.Execute(sql)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return r
}

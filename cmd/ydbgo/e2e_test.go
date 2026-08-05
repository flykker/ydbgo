package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "ydbgo")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func waitPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("port %s not reachable", addr)
}

func TestCLIServeAndRun(t *testing.T) {
	bin := buildBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	addr := "127.0.0.1:2399"

	srv := exec.Command(bin, "serve", "-addr", addr, "-data", dataDir)
	var srvOut bytes.Buffer
	srv.Stdout = &srvOut
	srv.Stderr = &srvOut
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		srv.Process.Kill()
		srv.Wait()
	}()

	waitPort(t, addr, 5*time.Second)

	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "run", "-addr", addr, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("run %q: %v\n%s", sql, err, out)
		}
		return string(out)
	}

	run("CREATE TABLE users (id int64 primary key, name string, age int64)")
	run("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")
	sel := run("SELECT name, age FROM users WHERE age >= 30 ORDER BY id")
	if !strings.Contains(sel, "Bob") || !strings.Contains(sel, "30") {
		t.Errorf("select output: %q", sel)
	}
	agg := run("SELECT COUNT(*) AS c FROM users")
	if !strings.Contains(agg, "2") {
		t.Errorf("aggregate output: %q", agg)
	}
	// batch file
	sqlFile := filepath.Join(t.TempDir(), "batch.sql")
	os.WriteFile(sqlFile, []byte("INSERT INTO users VALUES (3, 'Carol', 40);\nSELECT COUNT(*) AS c FROM users;\n"), 0o644)
	out, err := exec.Command(bin, "run", "-addr", addr, "@"+sqlFile).CombinedOutput()
	if err != nil {
		t.Fatalf("batch: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "3") {
		t.Errorf("batch output: %q", out)
	}
	_ = fmt.Sprintf
}

func TestCLIClusterJoinAndReplicate(t *testing.T) {
	bin := buildBinary(t)
	base := t.TempDir()
	addr1, addr2 := "127.0.0.1:2491", "127.0.0.1:2492"
	raft1, raft2 := "127.0.0.1:2493", "127.0.0.1:2494"

	start := func(args ...string) (*exec.Cmd, *bytes.Buffer) {
		full := append([]string{"serve"}, args...)
		cmd := exec.Command(bin, full...)
		var buf bytes.Buffer
		cmd.Stdout = &buf
		cmd.Stderr = &buf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, &buf
	}

	n1, buf1 := start("-addr", addr1, "-data", base+"/n1", "-raft-addr", raft1, "-node-id", "n1", "-bootstrap", "-rf", "2")
	defer n1.Process.Kill()
	waitPort(t, addr1, 5*time.Second)
	waitRaftLeader(t, bin, addr1, 8*time.Second)

	n2, buf2 := start("-addr", addr2, "-data", base+"/n2", "-raft-addr", raft2, "-node-id", "n2", "-join", addr1, "-rf", "2")
	defer n2.Process.Kill()
	waitPort(t, addr2, 5*time.Second)
	waitRaftLeader(t, bin, addr2, 10*time.Second)
	t.Logf("n2 log: %s", buf2.String())
	_ = buf1

	run := func(addr, sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "run", "-addr", addr, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("run %q: %v\n%s", sql, err, out)
		}
		return string(out)
	}

	// runUntil polls a query until its output contains want (replication lags).
	runUntil := func(addr, sql, want string, d time.Duration) {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			out := run(addr, sql)
			if strings.Contains(out, want) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		out, _ := exec.Command(bin, "run", "-addr", addr, sql).CombinedOutput()
		t.Errorf("%s via %s never showed %q: %q", sql, addr, want, out)
	}

	// write via n1, read via n2 (routing/scatter across both)
	if out, err := exec.Command(bin, "run", "-addr", addr1, "CREATE TABLE t (id int64 primary key, v string)").CombinedOutput(); err != nil {
		t.Fatalf("create: %v\nn1log: %s\nout: %s", err, buf1.String(), out)
	}
	run(addr1, "INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
	runUntil(addr2, "SELECT v FROM t WHERE id = 2", "world", 8*time.Second)
	// write via n2, read via n1
	run(addr2, "INSERT INTO t VALUES (3, 'three')")
	runUntil(addr1, "SELECT COUNT(*) AS c FROM t", "3", 8*time.Second)
	// each shard group replicated to both nodes (RF=2)
	shards := run(addr1, "ADMIN SHARDS t")
	if !strings.Contains(shards, "n1,n2") {
		t.Errorf("shard placement: %q", shards)
	}
	// manual split while both nodes up
	run(addr1, "ADMIN SPLIT TABLE t AT 2")
	shards2 := run(addr1, "ADMIN SHARDS t")
	if !strings.Contains(shards2, "2") {
		t.Errorf("after split: %q", shards2)
	}
	// data still intact and visible on both nodes
	for _, a := range []string{addr1, addr2} {
		cnt := run(a, "SELECT COUNT(*) AS c FROM t")
		if !strings.Contains(cnt, "3") {
			t.Errorf("count on %s after split: %q", a, cnt)
		}
	}
}

// waitRaftLeader polls until the node answers a trivial read (leader known).
func waitRaftLeader(t *testing.T, bin, addr string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		out, err := exec.Command(bin, "run", "-addr", addr, "SELECT 1 AS one").CombinedOutput()
		if err == nil && strings.Contains(string(out), "1") {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no leader on %s", addr)
}

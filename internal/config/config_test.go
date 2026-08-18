package config

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

const twoHosts = `config:
  hosts:
  - {host: 127.0.0.1, grpc: 2135, raft: 7001, data: ./ydbgo-data, id: n1, bootstrap: true}
  - {host: 127.0.0.1, grpc: 2136, raft: 7002, data: ./ydbgo-n2, id: n2}
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "cluster.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadTwoHosts(t *testing.T) {
	cfg, err := Load(writeConfig(t, twoHosts))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(cfg.Config.Hosts); got != 2 {
		t.Fatalf("hosts = %d, want 2", got)
	}
	h1 := cfg.Config.Hosts[0]
	if h1.ID != "n1" || h1.GRPC != 2135 || h1.Raft != 7001 || h1.Data != "./ydbgo-data" || !h1.Bootstrap {
		t.Fatalf("unexpected host n1: %+v", h1)
	}
	if h1.GRPCAddr() != "127.0.0.1:2135" || h1.RaftAddr() != "127.0.0.1:7001" {
		t.Fatalf("bad addrs: %s / %s", h1.GRPCAddr(), h1.RaftAddr())
	}
	h2 := cfg.Config.Hosts[1]
	if h2.Bootstrap {
		t.Fatalf("n2 must not be bootstrap")
	}
}

func TestLoadHostDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, `config:
  hosts:
  - {id: solo, grpc: 2135, raft: 7001, data: ./d, bootstrap: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	h := cfg.Config.Hosts[0]
	if h.Host != "127.0.0.1" {
		t.Fatalf("host default = %q, want 127.0.0.1", h.Host)
	}
	if h.GRPCAddr() != "127.0.0.1:2135" {
		t.Fatalf("grpc addr = %q", h.GRPCAddr())
	}
}

func TestLoadErrors(t *testing.T) {
	cases := map[string]string{
		"unknown top-level key": `metadata: {kind: MainConfig}`,
		"unknown host key": `config:
  hosts:
  - {id: n1, grpc: 1, raft: 2, data: ./d, bootstrap: true, bogus: 1}`,
		"no hosts": `config: {hosts: []}`,
		"missing id": `config:
  hosts:
  - {grpc: 1, raft: 2, data: ./d, bootstrap: true}`,
		"missing data": `config:
  hosts:
  - {id: n1, grpc: 1, raft: 2, bootstrap: true}`,
		"bad grpc port": `config:
  hosts:
  - {id: n1, grpc: 0, raft: 2, data: ./d, bootstrap: true}`,
		"duplicate id": `config:
  hosts:
  - {id: n1, grpc: 1, raft: 2, data: ./a, bootstrap: true}
  - {id: n1, grpc: 3, raft: 4, data: ./b}`,
		"no bootstrap": `config:
  hosts:
  - {id: n1, grpc: 1, raft: 2, data: ./a}`,
		"two bootstraps": `config:
  hosts:
  - {id: n1, grpc: 1, raft: 2, data: ./a, bootstrap: true}
  - {id: n2, grpc: 3, raft: 4, data: ./b, bootstrap: true}`,
		"bad yaml": `config: [oops`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, body)); err == nil {
				t.Fatalf("expected error for %s", name)
			}
		})
	}
}

func TestSelectNode(t *testing.T) {
	cfg, err := Load(writeConfig(t, twoHosts))
	if err != nil {
		t.Fatal(err)
	}
	if h, err := cfg.SelectNode("n2"); err != nil || h.ID != "n2" {
		t.Fatalf("SelectNode(n2) = %v, %v", h, err)
	}
	if _, err := cfg.SelectNode("nope"); err == nil {
		t.Fatal("expected error for unknown id")
	}
	if _, err := cfg.SelectNode(""); err == nil {
		t.Fatal("expected ambiguity error for empty id with 2 hosts")
	}

	one, err := Load(writeConfig(t, `config:
  hosts:
  - {id: solo, grpc: 2135, raft: 7001, data: ./d, bootstrap: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	if h, err := one.SelectNode(""); err != nil || h.ID != "solo" {
		t.Fatalf("SelectNode() on single host = %v, %v", h, err)
	}
}

func TestJoinTarget(t *testing.T) {
	cfg, err := Load(writeConfig(t, twoHosts))
	if err != nil {
		t.Fatal(err)
	}
	n1, _ := cfg.SelectNode("n1")
	n2, _ := cfg.SelectNode("n2")
	if got := cfg.JoinTarget(n1); got != "" {
		t.Fatalf("bootstrap node join = %q, want empty", got)
	}
	if got := cfg.JoinTarget(n2); got != "127.0.0.1:2135" {
		t.Fatalf("n2 join = %q, want 127.0.0.1:2135", got)
	}
}

func TestApplyPrecedence(t *testing.T) {
	cfg, err := Load(writeConfig(t, twoHosts))
	if err != nil {
		t.Fatal(err)
	}
	n2, _ := cfg.SelectNode("n2")
	join := cfg.JoinTarget(n2)

	// No explicit flags: config values win over built-in defaults.
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", ":9999", "")
	data := fs.String("data", "./default", "")
	raft := fs.String("raft-addr", "", "")
	id := fs.String("node-id", "", "")
	boot := fs.Bool("bootstrap", false, "")
	jn := fs.String("join", "", "")
	if err := Apply(fs, n2, join); err != nil {
		t.Fatal(err)
	}
	if *addr != "127.0.0.1:2136" || *data != "./ydbgo-n2" || *raft != "127.0.0.1:7002" || *id != "n2" || *boot || *jn != "127.0.0.1:2135" {
		t.Fatalf("apply: addr=%s data=%s raft=%s id=%s boot=%v join=%s",
			*addr, *data, *raft, *id, *boot, *jn)
	}

	// Explicit flag must survive: override data and join on the command line.
	fs2 := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr2 := fs2.String("addr", ":9999", "")
	data2 := fs2.String("data", "./default", "")
	_ = fs2.String("raft-addr", "", "")
	_ = fs2.String("node-id", "", "")
	_ = fs2.Bool("bootstrap", false, "")
	jn2 := fs2.String("join", "", "")
	if err := fs2.Parse([]string{"-data", "/custom/data", "-join", "1.2.3.4:2135"}); err != nil {
		t.Fatal(err)
	}
	if err := Apply(fs2, n2, join); err != nil {
		t.Fatal(err)
	}
	if *data2 != "/custom/data" {
		t.Fatalf("explicit data overridden: %s", *data2)
	}
	if *jn2 != "1.2.3.4:2135" {
		t.Fatalf("explicit join overridden: %s", *jn2)
	}
	if *addr2 != "127.0.0.1:2136" {
		t.Fatalf("config addr not applied: %s", *addr2)
	}

	// A node with no join target (empty join) must leave the join flag alone.
	fs3 := flag.NewFlagSet("serve", flag.ContinueOnError)
	jn3 := fs3.String("join", "keep-me", "")
	if err := Apply(fs3, n2, ""); err != nil {
		t.Fatal(err)
	}
	if *jn3 != "keep-me" {
		t.Fatalf("empty join clobbered the flag: %s", *jn3)
	}
}

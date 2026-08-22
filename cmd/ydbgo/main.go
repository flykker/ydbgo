package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"time"

	"ydbgo/internal/config"
	"ydbgo/internal/server"
)

const usage = `ydbgo — single-binary distributed SQL database (YDB-inspired)

Usage:
  ydbgo serve [-addr host:port] [-data DIR]                 start the database server
  ydbgo serve -raft-addr R:PORT [-node-id ID]               start as a cluster node
           [-bootstrap] [-join HOST:PORT] [-rf N] [-shard-size BYTES]
           [-split-check DURATION] [-recovery-check DURATION] [-ttl-tick DURATION]
           [-http :8080] [-pprof :6060]                     web console / pprof
  ydbgo serve -config cluster.yaml [-node-id ID]            start from a YDB-style
           cluster config: topology/addr/data/join are taken from the file,
           explicit flags still win
  ydbgo run [-addr host:port] SQL...                        execute SQL against a server
  ydbgo repl [-addr host:port]                              interactive shell
  ydbgo bench -addr host:port [-n N] [-rows R] [-c C] [-engine TABLE|KV|CSTORE|CSTORE2]
  ydbgo run -addr host:port @FILE.sql                       run statements from a fileExamples:
  ydbgo serve -addr :2135 -data ./data
  ydbgo serve -addr :2135 -data ./n1 -raft-addr 127.0.0.1:7001 -node-id n1 -bootstrap
  ydbgo serve -addr :2136 -data ./n2 -raft-addr 127.0.0.1:7002 -node-id n2 -join 127.0.0.1:2135
  ydbgo serve -config cluster.yaml -node-id n1
  ydbgo serve -config cluster.yaml -node-id n2
  ydbgo run -addr :2135 "CREATE TABLE users (id int64 primary key, v string)"
  ydbgo run -addr :2135 "ADMIN SHARDS users"
  ydbgo run -addr :2135 "ADMIN SPLIT TABLE users AT 500"
  ydbgo repl
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve", "server":
		runServe(os.Args[2:])
	case "run", "exec":
		runClient(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	case "updel":
		benchUpDel(os.Args[2:])
	case "repl", "shell":
		runRepl(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Print(usage)
		os.Exit(2)
	}
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "", "YDB-style cluster config (YAML); topology/addr/join come from it, explicit flags win")
	addr := fs.String("addr", ":2135", "listen address")
	data := fs.String("data", "./ydbgo-data", "data directory")
	id := fs.String("node-id", "", "raft node id (defaults to raft-addr)")
	raftAddr := fs.String("raft-addr", "", "raft listen address; empty disables clustering")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap a new single-node cluster")
	join := fs.String("join", "", "existing node to join (host:port of a live node)")
	rf := fs.Int("rf", 0, "replication factor for data shards (0 = all nodes)")
	shardSize := fs.Uint64("shard-size", 0, "auto-split threshold in bytes (0 = disabled)")
	splitTick := fs.Duration("split-check", 5*time.Second, "auto-split check interval")
	recoveryTick := fs.Duration("recovery-check", 0, "replica-heal check interval (0 = disabled)")
	ttlTick := fs.Duration("ttl-tick", 0, "auto-TTL purge check interval (0 = disabled)")
	pprofAddr := fs.String("pprof", "", "optional HTTP pprof listen address (e.g. :6060)")
	httpAddr := fs.String("http", "", "optional embedded web console listen address (e.g. :8080)")
	fs.Parse(args)
	if *pprofAddr != "" {
		startPprof(*pprofAddr)
	}
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(2)
		}
		node, err := cfg.SelectNode(*id)
		if err != nil {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(2)
		}
		if err := config.Apply(fs, node, cfg.JoinTarget(node)); err != nil {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(2)
		}
	}
	if *raftAddr == "" {
		if err := server.RunServer(*addr, *data, *httpAddr); err != nil {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(1)
		}
		return
	}
	if err := server.RunClusterServer(*addr, *data, *id, *raftAddr, *httpAddr, *bootstrap, *join, *rf, *shardSize, *splitTick, *recoveryTick, *ttlTick); err != nil {
		fmt.Fprintln(os.Stderr, "serve:", err)
		os.Exit(1)
	}
}

// applyClientConfig points run/repl/bench at the bootstrap node of a cluster
// config unless the user passed an explicit -addr.
func applyClientConfig(fs *flag.FlagSet, cfg *config.Config) error {
	b := cfg.BootstrapHost()
	if b == nil {
		return fmt.Errorf("config: no bootstrap host found")
	}
	return config.Apply(fs, b, "")
}

func runClient(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:2135", "server address")
	configPath := fs.String("config", "", "cluster config (default addr = bootstrap node)")
	fs.Parse(args)
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(2)
		}
		if err := applyClientConfig(fs, cfg); err != nil {
			fmt.Fprintln(os.Stderr, "run:", err)
			os.Exit(2)
		}
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "run: no SQL given")
		os.Exit(2)
	}
	if err := server.RunClient(*addr, rest); err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		os.Exit(1)
	}
}

func runRepl(args []string) {
	fs := flag.NewFlagSet("repl", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:2135", "server address")
	configPath := fs.String("config", "", "cluster config (default addr = bootstrap node)")
	fs.Parse(args)
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "repl:", err)
			os.Exit(2)
		}
		if err := applyClientConfig(fs, cfg); err != nil {
			fmt.Fprintln(os.Stderr, "repl:", err)
			os.Exit(2)
		}
	}
	if err := server.RunClient(*addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, "repl:", err)
		os.Exit(1)
	}
}

// startPprof exposes net/http/pprof so CPU/memory profiles can be captured from
// a running node via "go tool pprof http://host:port/debug/pprof/profile".
func startPprof(addr string) {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		fmt.Fprintf(os.Stderr, "pprof listening on %s\n", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Fprintln(os.Stderr, "pprof:", err)
		}
	}()
}

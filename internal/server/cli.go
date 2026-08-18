package server

import (
	"fmt"
	"net"
	"os"
	"time"

	"ydbgo/internal/proto"
	"ydbgo/internal/shard"
	"ydbgo/internal/storage"
	"ydbgo/internal/ui"
)

// RunServer starts a storage-backed server until interrupted.
func RunServer(addr, dir, httpAddr string) error {
	eng, err := storage.Open(dir)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer eng.Close()
	srv := NewServer(eng)
	if err := srv.Listen(addr); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "ydbgo listening on %s (data: %s)\n", srv.Addr(), dir)
	if httpAddr != "" {
		startUIServer(httpAddr, &standaloneBackend{srv})
	}
	return srv.Serve()
}

// RunClusterServer starts a sharded cluster node: a meta-group member plus
// the data shard groups this node hosts.
func RunClusterServer(addr, dataDir, id, raftAddr, httpAddr string, bootstrap bool, join string,
	rf int, shardSize uint64, splitTick, recoveryTick, ttlTick time.Duration) error {
	if id == "" {
		id = raftAddr
	}
	advertise, err := advertiseAddr(raftAddr)
	if err != nil {
		return err
	}
	mgr, err := shard.NewManager(shard.Config{
		ID:           id,
		SQLAddr:      addr,
		RaftAddr:     advertise,
		DataDir:      dataDir,
		RF:           rf,
		ShardSize:    shardSize,
		SplitTick:    splitTick,
		RecoveryTick: recoveryTick,
		TTLTick:      ttlTick,
	})
	if err != nil {
		return err
	}
	defer mgr.Close()
	if err := mgr.Start(bootstrap, join); err != nil {
		return err
	}
	srv := NewShardedServer(mgr)
	if err := srv.Listen(addr); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "ydbgo node %q serving on %s, meta raft on %s (data: %s)\n", id, srv.Addr(), advertise, dataDir)
	if httpAddr != "" {
		startUIServer(httpAddr, mgr)
	}
	return srv.Serve()
}

// startUIServer runs the embedded web console on httpAddr in a background
// goroutine; it shares the node process.
func startUIServer(httpAddr string, backend ui.Backend) {
	uiSrv := ui.NewServer(backend)
	go func() {
		fmt.Fprintf(os.Stderr, "web console on http://%s\n", httpAddr)
		if err := uiSrv.Serve(httpAddr); err != nil {
			fmt.Fprintf(os.Stderr, "ui: %v\n", err)
		}
	}()
}

// standaloneBackend adapts the non-sharded server to the UI backend. Cluster
// views (tables/nodes/metrics) are empty here; SQL/query still works.
type standaloneBackend struct {
	srv *Server
}

func (b *standaloneBackend) Handle(req *proto.Request) *proto.Response { return b.srv.Handle(req) }
func (b *standaloneBackend) Tables() []proto.TableInfo                 { return b.srv.StandaloneTables() }
func (b *standaloneBackend) Shards(table string) ([]proto.ShardInfo, error) {
	return b.srv.StandaloneShards(table)
}
func (b *standaloneBackend) Nodes() []proto.NodeInfo { return nil }
func (b *standaloneBackend) NodeMetrics() []proto.NodeMetrics {
	return []proto.NodeMetrics{{Node: "local", Addr: b.srv.Addr().String(), Status: "up", JSON: b.srv.met.json()}}
}

func advertiseAddr(raftAddr string) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("raft-addr %q: %w", raftAddr, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "7001"
	}
	return net.JoinHostPort(host, port), nil
}

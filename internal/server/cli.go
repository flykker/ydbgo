package server

import (
	"fmt"
	"net"
	"os"
	"time"

	"ydbgo/internal/shard"
	"ydbgo/internal/storage"
)

// RunServer starts a storage-backed server until interrupted.
func RunServer(addr, dir string) error {
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
	return srv.Serve()
}

// RunClusterServer starts a sharded cluster node: a meta-group member plus
// the data shard groups this node hosts.
func RunClusterServer(addr, dataDir, id, raftAddr string, bootstrap bool, join string,
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
	return srv.Serve()
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

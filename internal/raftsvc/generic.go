package raftsvc

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

// noDelayStreamLayer is a raft.StreamLayer over plain TCP that enables
// TCP_NODELAY on every accepted and dialed connection. hashicorp/raft leaves
// Nagle on by default, which inflates each small AppendEntries round trip by
// up to 40ms on loopback (and on LANs with Nagle) — exactly the traffic a
// high-rate group-commit write path produces.
type noDelayStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

func (l *noDelayStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	c, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return c, nil
}

func (l *noDelayStreamLayer) Accept() (net.Conn, error) {
	c, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return c, nil
}

func (l *noDelayStreamLayer) Close() error { return l.listener.Close() }

func (l *noDelayStreamLayer) Addr() net.Addr {
	if l.advertise != nil {
		return l.advertise
	}
	return l.listener.Addr()
}

// newNoDelayTransport builds a raft network transport whose connections all
// run with TCP_NODELAY enabled.
func newNoDelayTransport(addr string, advertise net.Addr, timeout time.Duration) (*raft.NetworkTransport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	stream := &noDelayStreamLayer{advertise: advertise, listener: ln.(*net.TCPListener)}
	return raft.NewNetworkTransport(stream, 3, timeout, nil), nil
}

// Group is a generic Raft group: a transport, stores and a leader-election
// lifecycle. It is the shared core for both the meta/catalog group and the
// per-shard data groups. Application state lives in the FSM.
type Group struct {
	id        string
	addr      string
	dir       string
	fsm       raft.FSM
	raft      *raft.Raft
	advertise raft.ServerAddress
	closers   []func() error
}

// NewGroup creates (but does not start) a Raft group using fsm as state.
// If dir is non-empty the group persists its log, stable store and snapshots
// under dir (used by the meta group and restartable data groups); otherwise
// it runs fully in memory.
func NewGroup(id, raftAddr, dir string, fsm raft.FSM) *Group {
	return &Group{id: id, addr: raftAddr, dir: dir, fsm: fsm}
}

// Start configures and starts the Raft instance.
// If bootstrap is true the group initializes a single-node cluster.
// Otherwise it starts as a follower; use AddPeer on the leader to join it.
func (g *Group) Start(bootstrap bool, peers []raft.Server) error {
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(g.id)
	conf.SnapshotInterval = 5 * time.Minute
	conf.SnapshotThreshold = 100000

	advertise, err := newNoDelayTransport(g.addr, nil, 5*time.Second)
	if err != nil {
		return err
	}
	g.advertise = advertise.LocalAddr()

	var logStore raft.LogStore
	var stableStore raft.StableStore
	var snapStore raft.SnapshotStore
	if g.dir != "" {
		if err := os.MkdirAll(g.dir, 0o755); err != nil {
			return err
		}
		ls, err := newFileLogStore(filepath.Join(g.dir, "raft"))
		if err != nil {
			return err
		}
		ss, err := newFileStableStore(filepath.Join(g.dir, "raft"))
		if err != nil {
			return err
		}
		snaps, err := raft.NewFileSnapshotStore(g.dir, 3, nil)
		if err != nil {
			return err
		}
		logStore, stableStore, snapStore = ls, ss, snaps
		g.closers = append(g.closers, ls.Close, ss.Close)
	} else {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
		snapStore = raft.NewInmemSnapshotStore()
	}

	r, err := raft.NewRaft(conf, g.fsm, logStore, stableStore, snapStore, advertise)
	if err != nil {
		return err
	}
	g.raft = r
	if bootstrap {
		cfg := raft.Configuration{Servers: []raft.Server{
			{ID: raft.ServerID(g.id), Address: g.advertise},
		}}
		r.BootstrapCluster(cfg)
	} else {
		for _, s := range peers {
			f := r.AddVoter(raft.ServerID(s.ID), s.Address, 0, 0)
			f.Error()
		}
	}
	return nil
}

// Advertise returns the transport address this member advertises.
func (g *Group) Advertise() string { return string(g.advertise) }

// AddPeer adds a voter to the group. Call on the leader.
func (g *Group) AddPeer(id string, addr string) error {
	if g.raft == nil {
		return errors.New("raft not started")
	}
	f := g.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 5*time.Second)
	return f.Error()
}

// RemovePeer removes a voter from the group. Call on the leader.
func (g *Group) RemovePeer(id string) error {
	if g.raft == nil {
		return errors.New("raft not started")
	}
	f := g.raft.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
	return f.Error()
}

// IsLeader reports whether this member is the current leader.
func (g *Group) IsLeader() bool {
	return g.raft != nil && g.raft.State() == raft.Leader
}

// LeaderAddr returns the transport address of the current leader.
func (g *Group) LeaderAddr() string {
	if g.raft == nil {
		return ""
	}
	addr, _ := g.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current leader.
func (g *Group) LeaderID() string {
	if g.raft == nil {
		return ""
	}
	_, id := g.raft.LeaderWithID()
	return string(id)
}

// Peers lists the configured voters.
func (g *Group) Peers() []Peer {
	if g.raft == nil {
		return nil
	}
	var out []Peer
	for _, s := range g.raft.GetConfiguration().Configuration().Servers {
		out = append(out, Peer{ID: string(s.ID), Address: s.Address})
	}
	return out
}

// Raft exposes the underlying raft instance (advanced use).
func (g *Group) Raft() *raft.Raft { return g.raft }

// Close shuts down the group.
func (g *Group) Close() error {
	var first error
	if g.raft != nil {
		if err := g.raft.Shutdown().Error(); err != nil {
			first = err
		}
	}
	for _, c := range g.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

package raftsvc

import (
	"errors"
	"io"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	sqlx "ydbgo/internal/sql"
	"ydbgo/internal/storage"
)

// FSM applies replicated SQL write statements to the storage engine.
type FSM struct {
	eng  *storage.Engine
	exec *sqlx.Executor
}

func NewFSM(eng *storage.Engine) *FSM {
	return &FSM{eng: eng, exec: sqlx.NewExecutor(eng)}
}

// Apply implements raft.FSM. The payload is one or more SQL write
// statements separated by ';' (a group-committed batch). All statements of
// the batch execute inside ONE durable store transaction, so the batch costs
// a single storage fsync (group commit at the storage layer).
func (f *FSM) Apply(l *raft.Log) interface{} {
	stmts, err := sqlx.Parse(string(l.Data))
	if err != nil {
		return err
	}
	if len(stmts) == 0 {
		return nil
	}
	err = f.eng.UpdateBatch(func() error {
		for _, st := range stmts {
			if _, err := f.exec.Execute(st); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Snapshot serializes the current engine state as the FSM snapshot payload.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.eng.MarshalState()
	if err != nil {
		return nil, err
	}
	return &snapshot{data: data}, nil
}

// Restore rebuilds the engine from a snapshot payload and rewrites the WAL
// to match, so subsequent raft log replay appends on top of the restored state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return err
	}
	return f.eng.ReplaceState(data)
}

type snapshot struct {
	data []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *snapshot) Release() {}

// Node is a Raft-backed replica of a single table's data (one shard group)
// or, in the legacy single-group mode, the whole database.
type Node struct {
	id       string
	raftAddr string
	dataDir  string
	group    *Group
	fsm      *FSM
	eng      *storage.Engine
	exec     *sqlx.Executor
	batch    *batcher
}

// Config describes a node's Raft participation.
type Config struct {
	ID          string
	RaftAddr    string // e.g. "127.0.0.1:7001"
	Peers       []string
	DataDir     string
	LocalID     string
	Bootstrap   bool // bootstrap a new single-node cluster
	JoinTimeout time.Duration
}

// NewNode creates (but does not start) a Raft node.
func NewNode(cfg Config) (*Node, error) {
	eng, err := storage.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	// Raft-backed: the raft log (fileLogStore.StoreLogs) now owns durability
	// via its own fsync, so the engine WAL must not double-fsync every mutate.
	eng.SetNoSync(true)
	fsm := NewFSM(eng)
	n := &Node{
		id:       cfg.ID,
		raftAddr: cfg.RaftAddr,
		dataDir:  cfg.DataDir,
		fsm:      fsm,
		eng:      eng,
		exec:     sqlx.NewExecutor(eng),
	}
	n.group = NewGroup(cfg.ID, cfg.RaftAddr, cfg.DataDir, fsm)
	n.batch = newBatcher(func() *raft.Raft { return n.group.Raft() })
	return n, nil
}

func (n *Node) Engine() *storage.Engine  { return n.eng }
func (n *Node) Executor() *sqlx.Executor { return n.exec }
func (n *Node) Raft() *raft.Raft         { return n.group.Raft() }
func (n *Node) Group() *Group            { return n.group }

// Start configures and starts the Raft instance.
// If bootstrap is true the node initializes a single-node cluster.
// Otherwise it starts as a follower; use AddPeer on the leader to join it.
func (n *Node) Start(bootstrap bool, peers []raft.Server) error {
	if err := n.group.Start(bootstrap, peers); err != nil {
		return err
	}
	n.batch.Start()
	return nil
}

// AddPeer adds a voter to the cluster. Call on the leader.
func (n *Node) AddPeer(id string, addr string) error {
	return n.group.AddPeer(id, addr)
}

// RemovePeer removes a voter from the cluster. Call on the leader.
func (n *Node) RemovePeer(id string) error {
	return n.group.RemovePeer(id)
}

// Submit proposes a write statement to the cluster and waits for commit.
func (n *Node) Submit(sql string) error {
	stmts, err := sqlx.Parse(sql)
	if err != nil {
		return err
	}
	for _, st := range stmts {
		if isRead(st) {
			continue
		}
		if err := n.submitOne(st); err != nil {
			return err
		}
	}
	return nil
}

// submitOne ships a single write statement through the group-commit batcher.
func (n *Node) submitOne(st sqlx.Statement) error {
	if n.group == nil || !n.group.IsLeader() {
		return errors.New("not leader")
	}
	return n.batch.submit(sqlx.StatementString(st))
}

// Execute runs a statement: writes go through Raft, reads run locally.
func (n *Node) Execute(sql string) (*sqlx.Result, error) {
	stmts, err := sqlx.Parse(sql)
	if err != nil {
		return nil, err
	}
	var last *sqlx.Result
	for _, st := range stmts {
		if isRead(st) {
			r, err := n.exec.Execute(st)
			if err != nil {
				return nil, err
			}
			last = r
			continue
		}
		if err := n.submitOne(st); err != nil {
			return nil, err
		}
		last = &sqlx.Result{Type: resultType(st)}
	}
	return last, nil
}

func (n *Node) IsLeader() bool { return n.group.IsLeader() }

func (n *Node) LeaderAddr() string { return n.group.LeaderAddr() }

// Close shuts down Raft and storage.
func (n *Node) Close() error {
	n.batch.Stop()
	if err := n.group.Close(); err != nil {
		return err
	}
	return n.eng.Close()
}

func isRead(st sqlx.Statement) bool {
	switch st.(type) {
	case *sqlx.SelectStmt:
		return true
	}
	return false
}

func resultType(st sqlx.Statement) string {
	switch st.(type) {
	case *sqlx.CreateTableStmt:
		return "create_table"
	case *sqlx.DropTableStmt:
		return "drop_table"
	case *sqlx.InsertStmt:
		return "insert"
	case *sqlx.UpdateStmt:
		return "update"
	case *sqlx.DeleteStmt:
		return "delete"
	case *sqlx.CreateIndexStmt:
		return "create_index"
	case *sqlx.DropIndexStmt:
		return "drop_index"
	case *sqlx.CreateDatabaseStmt:
		return "create_database"
	}
	return "ok"
}

// Leader helpers.

type Peer struct {
	ID      string
	Address raft.ServerAddress
}

func (n *Node) Peers() []Peer { return n.group.Peers() }

var _ = strings.TrimSpace

package raftsvc

import (
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	sqlx "ydbgo/internal/sql"
)

// Batch config bounds for group-committed raft entries.
const (
	batchWindowMs = 1    // max time to fill a batch before flushing
	batchMaxOps   = 4096 // max statements folded into one raft entry
	batchBuf      = 512  // buffered enqueue ahead of the flush loop
	applyTimeout  = 10 * time.Second
)

// batchItem is one caller's write statement waiting for commit.
type batchItem struct {
	st  sqlx.Statement
	res chan *sqlx.Result
	err chan error
}

// batcher coalesces concurrent leader writes into a single raft.Apply per
// short window (group commit). Concurrent writes that used to cost one
// fsync + one quorum round-trip each now share a single raft entry, so N
// writes cost ~1 fsync + 1 round-trip per batch. Durability is unchanged:
// every statement is still committed to a majority before its caller returns.
//
// Entries carry the leader's parsed statements in a compact binary encoding
// (sqlx.EncodeStatements) rather than SQL text: followers apply without
// re-parsing and the entry is smaller on the wire and in the raft log.
type batcher struct {
	getRaft func() *raft.Raft

	window time.Duration
	maxOps int

	ch       chan *batchItem
	close    chan struct{}
	stopOnce sync.Once
}

func newBatcher(get func() *raft.Raft) *batcher {
	return &batcher{
		getRaft: get,
		window:  batchWindowMs * time.Millisecond,
		maxOps:  batchMaxOps,
		ch:      make(chan *batchItem, batchBuf),
		close:   make(chan struct{}),
	}
}

// Start launches the flush loop.
func (b *batcher) Start() { go b.loop() }

// Stop ends the flush loop (idempotent).
func (b *batcher) Stop() {
	b.stopOnce.Do(func() { close(b.close) })
}

// submit enqueues a statement and blocks until it commits. While the flush
// loop is inside a raft.Apply the buffer absorbs backpressure; callers wait
// for their commit either way, matching pre-batch semantics.
func (b *batcher) submit(st sqlx.Statement) (*sqlx.Result, error) {
	item := &batchItem{st: st, res: make(chan *sqlx.Result, 1), err: make(chan error, 1)}
	b.ch <- item
	select {
	case res := <-item.res:
		return res, nil
	case err := <-item.err:
		return nil, err
	}
}

// loop drains the window, folding statements into one apply per flush.
func (b *batcher) loop() {
	for {
		select {
		case <-b.close:
			return
		case first := <-b.ch:
			b.flush(first)
		}
	}
}

// flush gathers up to maxOps statements arriving within the window around the
// first pending op and submits them as a single raft entry.
func (b *batcher) flush(first *batchItem) {
	pending := []*batchItem{first}
	window := time.NewTimer(b.window)
collect:
	for {
		select {
		case it := <-b.ch:
			pending = append(pending, it)
			if len(pending) >= b.maxOps {
				break collect
			}
		case <-window.C:
			break collect
		}
	}
	b.apply(pending)
}

// apply submits all pending statements as one raft.Apply and fans out the
// result to the waiting callers.
func (b *batcher) apply(pending []*batchItem) {
	r := b.getRaft()
	if r == nil {
		b.fail(pending, errors.New("raft not started"))
		return
	}
	stmts := make([]sqlx.Statement, 0, len(pending))
	for _, it := range pending {
		stmts = append(stmts, it.st)
	}
	payload := sqlx.EncodeStatements(stmts)
	f := r.Apply(payload, applyTimeout)
	err := f.Error()
	var results []*sqlx.Result
	if err == nil {
		if resp, ok := f.Response().([]*sqlx.Result); ok {
			results = resp
		} else if respErr, ok := f.Response().(error); ok {
			err = respErr
		}
	}
	if err != nil {
		b.fail(pending, err)
		return
	}
	for i, it := range pending {
		var res *sqlx.Result
		if i < len(results) {
			res = results[i]
		}
		it.res <- res
	}
}

func (b *batcher) fail(pending []*batchItem, err error) {
	for _, it := range pending {
		it.err <- err
	}
}
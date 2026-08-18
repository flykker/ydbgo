package raftsvc

import (
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	sqlx "ydbgo/internal/sql"
)

// Batch config bounds for group-committed raft entries. The defaults can be
// overridden with YDBGO_BATCH_WINDOW_MS / YDBGO_BATCH_MAXOPS /
// YDBGO_BATCH_HARD_WINDOW_MS.
const (
	batchWindowMs   = 1    // quiet-gap: max idle time before flushing a batch
	batchMaxOps     = 4096 // max statements folded into one raft entry
	batchHardWindow = 4    // latency bound (ms) under sustained load
	batchBuf        = 512  // buffered enqueue ahead of the flush loop
	applyTimeout    = 10 * time.Second
)

func envMs(name string, def int) time.Duration {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(def) * time.Millisecond
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

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

	window time.Duration // quiet-gap between arrivals before flushing
	hard   time.Duration // latency bound for a batch under sustained load
	maxOps int

	ch       chan *batchItem
	close    chan struct{}
	stopOnce sync.Once
}

func newBatcher(get func() *raft.Raft) *batcher {
	return &batcher{
		getRaft: get,
		window:  envMs("YDBGO_BATCH_WINDOW_MS", batchWindowMs),
		hard:    envMs("YDBGO_BATCH_HARD_WINDOW_MS", batchHardWindow),
		maxOps:  envInt("YDBGO_BATCH_MAXOPS", batchMaxOps),
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
			// Nothing else is queued: apply right away instead of paying the
			// batch window for a single statement (idle writes get no
			// artificial latency; busy periods still coalesce in flush).
			if len(b.ch) == 0 {
				b.apply([]*batchItem{first})
				continue
			}
			b.flush(first)
		}
	}
}

// flush gathers up to maxOps statements and submits them as a single raft
// entry. Collection uses an adaptive quiet-gap window: it keeps draining the
// queue while statements keep arriving within `window` of the last one, so
// concurrent back-to-back writes pack into one batch (fewer raft entries,
// fewer fsyncs), and flushes once a quiet gap of `window` passes, the batch
// fills, or the hard latency bound is hit.
func (b *batcher) flush(first *batchItem) {
	pending := []*batchItem{first}
	quiet := time.NewTimer(b.window)
	defer quiet.Stop()
	hard := time.NewTimer(b.hard)
	defer hard.Stop()
collect:
	for {
		select {
		case it := <-b.ch:
			pending = append(pending, it)
			if len(pending) >= b.maxOps {
				break collect
			}
			// Reset the quiet gap: arrivals keep the batch open.
			if !quiet.Stop() {
				select {
				case <-quiet.C:
				default:
				}
			}
			quiet.Reset(b.window)
		case <-quiet.C:
			break collect
		case <-hard.C:
			break collect
		}
	}
	b.apply(pending)
}

// apply submits all pending statements as one raft.Apply and fans out the
// result to the waiting callers. The commit is awaited in a goroutine so the
// flush loop can pipeline the next batch: raft preserves log order and each
// caller still blocks on its own commit, but the quorum/fsync latency of
// consecutive entries overlaps instead of serializing.
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
	go func() {
		// Watchdog: if the raft entry never commits (leader can't reach
		// quorum), surface the raft state once instead of hanging silently.
		type result struct {
			err  error
			done bool
		}
		ch := make(chan result, 1)
		go func() {
			err := f.Error()
			ch <- result{err: err, done: true}
		}()
		select {
		case res := <-ch:
			b.completeApply(pending, f, res.err)
		case <-time.After(applyTimeout):
			log.Printf("BATCH-STALL: raft entry %s (%d ops) not committed within %s; stats: %v",
				r.String(), len(pending), applyTimeout, r.Stats())
			select {
			case res := <-ch:
				b.completeApply(pending, f, res.err)
			case <-time.After(60 * time.Second):
				log.Printf("BATCH-STALL-DEAD: still waiting after 60s for raft entry %s", r.String())
				b.completeApply(pending, f, errors.New("raft apply stalled"))
			}
		}
	}()
}

func (b *batcher) completeApply(pending []*batchItem, f raft.ApplyFuture, err error) {
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

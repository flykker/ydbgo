package kv

import (
	"bytes"
	"sync"
	"time"
)

// nowNanos is the current time as uint64 nanoseconds, used for lease expiry.
func nowNanos() uint64 { return uint64(time.Now().UnixNano()) }

// Watch groundwork: a hub that delivers commit notifications to subscribers.
// The delivery model is a simple prefix-filtered broadcast; higher layers
// (change feeds, replication hooks) can build on it. Watchers get the ops
// that touched a key with their prefix at the committed revision.

// Event describes one key changed in a committed batch.
type Event struct {
	Rev    Revision
	Key    []byte
	Value  []byte
	Delete bool
}

// subscription is one watcher's registration.
type subscription struct {
	prefix    []byte
	ch        chan Event
	done      chan struct{}
	closeOnce sync.Once
}

// hub fans out committed changes to subscribers. notify is called under the
// store lock after a batch commits; it must never block, so slow watchers
// get a buffered channel and may drop events.
type hub struct {
	mu   sync.Mutex
	subs map[*subscription]struct{}
}

func newHub() *hub { return &hub{subs: map[*subscription]struct{}{}} }

func (h *hub) close() {
	h.mu.Lock()
	subs := make([]*subscription, 0, len(h.subs))
	for s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()
	for _, s := range subs {
		s.close()
	}
}

func (h *hub) subscribe(prefix []byte, buf int) *subscription {
	s := &subscription{
		prefix: append([]byte(nil), prefix...),
		ch:     make(chan Event, buf),
		done:   make(chan struct{}),
	}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *hub) unsubscribe(s *subscription) {
	h.mu.Lock()
	_, ok := h.subs[s]
	if ok {
		delete(h.subs, s)
	}
	h.mu.Unlock()
	if ok {
		s.close()
	}
}

func (s *subscription) close() {
	s.closeOnce.Do(func() { close(s.done) })
}

// notify broadcasts committed ops to matching subscriptions.
func (h *hub) notify(rev Revision, ops []Op) {
	h.mu.Lock()
	subs := make([]*subscription, 0, len(h.subs))
	for s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()
	for _, op := range ops {
		for _, s := range subs {
			if !bytes.HasPrefix(op.Key, s.prefix) {
				continue
			}
			ev := Event{Rev: rev, Key: append([]byte(nil), op.Key...), Delete: op.Delete}
			if !op.Delete {
				ev.Value = append([]byte(nil), op.Value...)
			}
			select {
			case s.ch <- ev:
			default: // slow watcher: drop rather than block the write path
			}
		}
	}
}

// Watch subscribes to changes on keys with the given byte prefix. Watch has a
// non-blocking delivery contract: Events returns the channel with a bounded
// buffer; a watcher that does not drain quickly may miss events.
func (s *Store) Watch(prefix []byte) (<-chan Event, *subscription) {
	sub := s.hub.subscribe(prefix, 64)
	return sub.ch, sub
}

// CloseWatcher unregisters a watcher returned by Watch.
func (s *Store) CloseWatcher(sub *subscription) { s.hub.unsubscribe(sub) }
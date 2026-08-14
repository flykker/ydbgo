// Package kv is the byte-KV + MVCC foundation. It is a versioned key/value
// store over an embedded LSM (pebble): every write lands at a monotonically
// increasing Revision (per shard the revision is the Raft log index), and
// reads may be pinned to any past revision. Keys and values are raw bytes;
// ordering is bytewise, so range and prefix scans are natural.
//
// The package is deliberately self-contained (no SQL, no Raft types): it is
// the storage substrate for the ENGINE=KV tables and the groundwork for
// watch / CAS / lease semantics that higher layers will build on.
package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Revision is a monotonic version number. Higher = newer. Revision 0 means
// "latest" on reads.
type Revision uint64

// physical layout inside pebble:
//
//	\x00 lastrev                   meta: last committed revision (8B BE)
//	\x00 lease \x01 <id>           lease meta: expiry (8B BE)  (groundwork)
//	\x01 <key> \x00 <revSuffix>    one version of one key (8B suffix)
//
// The per-version key is `<key>\x00<revSuffix>` where revSuffix is the 8-byte
// big-endian encoding of ^rev. Larger revisions get SMALLER suffixes, so
// within a key's group the newest version sorts first; the key's trailing
// \x00 keeps different keys' groups separated even when one key is a byte
// prefix of another.
const (
	metaTag      byte = '\x00'
	dataTag      byte = '\x01'
	flagLive     byte = '\x01'
	flagTomb     byte = '\x00'
	sepSize           = 1 // trailing \x00 in a data key
	suffixSize        = 8 // revSuffix bytes
	metaLast          = "lastrev"
	metaLeaseSeq      = "leaseSeq" // last-issued lease id (monotonic)
	metaLeaseID       = "lease\x01"
)

var (
	// ErrRevRegress is returned when Apply is given a revision that is not
	// strictly greater than the previously applied one.
	ErrRevRegress = errors.New("kv: revision not greater than last applied")
	// ErrNoKey is returned by CAS when comparing against a not-found key.
	ErrNoKey = errors.New("kv: key not found")
)

// Store is a versioned byte-KV.
type Store struct {
	db   *pebble.DB
	mu   sync.Mutex // serializes Apply (one writer per shard)
	last Revision   // cached last committed revision
	hub  *hub
}

// Open opens (or creates) a versioned byte-KV at dir.
func Open(dir string) (*Store, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	db, err := pebble.Open(filepath.Join(dir, "ydb.kv"), opts)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, last: 0}
	s.hub = newHub()
	last, _ := s.readMeta(metaLast)
	if len(last) == 8 {
		s.last = Revision(binary.BigEndian.Uint64(last))
	}
	return s, nil
}

// Close releases the store.
func (s *Store) Close() error {
	s.hub.close()
	return s.db.Close()
}

// Latest returns the last committed revision (0 if the store is empty).
func (s *Store) Latest() Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// Op is a single mutation to apply at a revision.
type Op struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Apply atomically applies ops at revision rev. rev must be strictly greater
// than the previously applied revision (Raft guarantees this since index
// increments monotonically). Durability belongs to the raft log, so the
// commit uses pebble.NoSync.
func (s *Store) Apply(rev Revision, ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyLocked(rev, ops)
}

func (s *Store) applyLocked(rev Revision, ops []Op) error {
	if rev == 0 {
		rev = s.last + 1
	}
	if rev <= s.last {
		return ErrRevRegress
	}
	b := s.db.NewBatch()
	defer b.Close()
	for _, op := range ops {
		var v []byte
		if op.Delete {
			v = []byte{flagTomb}
		} else {
			v = make([]byte, 0, len(op.Value)+1)
			v = append(v, flagLive)
			v = append(v, op.Value...)
		}
		if err := b.Set(dataKey(op.Key, rev), v, nil); err != nil {
			return err
		}
	}
	var lb [8]byte
	binary.BigEndian.PutUint64(lb[:], uint64(rev))
	if err := b.Set(metaKey(metaLast), lb[:], nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	s.last = rev
	s.hub.notify(rev, ops)
	return nil
}

// Compact physically removes superseded versions, keeping the state as of
// revision keep and everything newer: for each key it retains the newest
// version with rev <= keep (the "anchor") and drops every older version, so
// reads at any revision >= keep observe the same values as before. The
// store's latest revision (s.last) is unchanged. Returns the number of
// version records dropped.
func (s *Store) Compact(keep Revision) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == 0 {
		return 0, nil
	}
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dataTag},
		UpperBound: []byte{dataTag + 1},
	})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	// Within a key group the newest version (smallest suffix = largest rev)
	// sorts first. Per group, the first version with rev <= keep becomes the
	// anchor and everything after it is dropped.
	var del [][]byte
	var lastKey []byte
	anchorSet := false
	var dropped int64
	it.First()
	for ; it.Valid(); it.Next() {
		dk := it.Key()
		raw := extractKey(dk)
		if !bytes.Equal(raw, lastKey) {
			lastKey = append([]byte(nil), raw...)
			anchorSet = false
		}
		rev := Revision(^binary.BigEndian.Uint64(dk[len(dk)-suffixSize:]))
		if rev > keep {
			continue // newer than keep: keep
		}
		if !anchorSet {
			anchorSet = true // newest version <= keep: the anchor, keep it
			continue
		}
		del = append(del, append([]byte(nil), dk...))
		dropped++
	}
	if len(del) > 0 {
		b := s.db.NewBatch()
		defer b.Close()
		for _, k := range del {
			if err := b.Delete(k, nil); err != nil {
				return dropped, err
			}
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return dropped, err
		}
	}
	return dropped, nil
}

// Get returns the value of key as of revision rev (0 = latest). ok is false
// when the key has never existed, or was deleted (tombstoned) at or before
// rev.
func (s *Store) Get(rev Revision, key []byte) ([]byte, bool, error) {
	R := s.effective(rev)
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: dataKey(key, R),
		UpperBound: dataUpper(key),
	})
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	it.SeekGE(dataKey(key, R))
	if !it.Valid() {
		return nil, false, it.Error()
	}
	if !bytes.Equal(extractKey(it.Key()), key) {
		return nil, false, nil
	}
	val := it.Value()
	if len(val) == 0 || val[0] == flagTomb {
		return nil, false, nil
	}
	out := append([]byte(nil), val[1:]...)
	return out, true, nil
}

// Range iterates keys in [start, end) at revision rev (0 = latest), in byte
// order. fn receives the key, its value as of rev and whether it is a
// tombstone (deleted) version. The newest version not newer than rev wins.
func (s *Store) Range(rev Revision, start, end []byte, fn func(key, val []byte, deleted bool) error) error {
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil // empty or inverted range: nothing to iterate
	}
	R := s.effective(rev)
	lower := []byte{dataTag}
	if start != nil {
		// Seek to the group boundary (no revision suffix): the starting key may
		// only be a prefix of the real keys below it (range/prefix scans), so a
		// dataKey(start, R) bound could sort past every matching version. The
		// per-key reposition below pins the read to revision R.
		lower = append(lower, start...)
		lower = append(lower, 0)
	}
	var upper []byte
	if end != nil {
		upper = dataUpper(end)
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	it.SeekGE(lower)
	if R == s.last {
		// Latest-revision scan: within each key's group the newest version
		// sorts first and dataUpper(raw) jumps straight to the next key's
		// newest version, so no per-key reposition is needed.
		var upperBuf []byte
		for it.Valid() {
			raw := extractKey(it.Key())
			if end != nil && bytes.Compare(raw, end) >= 0 {
				break
			}
			val := it.Value()
			deleted := len(val) == 0 || val[0] == flagTomb
			var payload []byte
			if !deleted {
				payload = append([]byte(nil), val[1:]...)
			}
			if err := fn(raw, payload, deleted); err != nil {
				return err
			}
			upperBuf = dataUpperInto(upperBuf[:0], raw)
			it.SeekGE(upperBuf)
		}
		return it.Error()
	}
	for it.Valid() {
		raw := extractKey(it.Key())
		if end != nil && bytes.Compare(raw, end) >= 0 {
			break
		}
		// The group's top may be a version newer than rev. Reposition inside
		// this key's group to the newest version that is <= rev.
		it.SeekGE(dataKey(raw, R))
		if !it.Valid() || !bytes.Equal(extractKey(it.Key()), raw) {
			// raw has no version <= rev (only newer ones); move to the next
			// naturally present key without emitting this one.
			continue
		}
		val := it.Value()
		deleted := len(val) == 0 || val[0] == flagTomb
		var payload []byte
		if !deleted {
			payload = append([]byte(nil), val[1:]...)
		}
		if err := fn(raw, payload, deleted); err != nil {
			return err
		}
		// jump the rest of this key's group (newest-first order)
		it.SeekGE(dataUpper(raw))
	}
	return it.Error()
}

// Prefix iterates every key with the given byte prefix at revision rev.
func (s *Store) Prefix(rev Revision, prefix []byte, fn func(key, val []byte, deleted bool) error) error {
	return s.Range(rev, prefix, nextKey(prefix), fn)
}

// RangeDesc iterates keys in [start, end) at revision rev (0 = latest), in
// reverse byte order. fn receives the key, its value as of rev and whether it
// is a tombstone (deleted) version. The newest version not newer than rev wins.
func (s *Store) RangeDesc(rev Revision, start, end []byte, fn func(key, val []byte, deleted bool) error) error {
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil // empty or inverted range: nothing to iterate
	}
	R := s.effective(rev)
	lower := []byte{dataTag}
	if start != nil {
		lower = append(lower, start...)
		lower = append(lower, 0)
	}
	var upper []byte
	if end != nil {
		upper = dataUpper(end)
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	// position at the last key strictly below the range end
	if upper != nil {
		it.SeekGE(upper)
		if !it.Valid() {
			it.Last()
		} else {
			it.Prev()
		}
	} else {
		it.Last()
	}
	for it.Valid() {
		raw := extractKey(it.Key())
		if start != nil && bytes.Compare(raw, start) < 0 {
			break
		}
		if end != nil && bytes.Compare(raw, end) >= 0 {
			// the exclusive end is not pruned by dataUpper(end) when raw == end
			belowGroup(it, raw)
			continue
		}
		// reposition at the newest version of this key that is <= R; within a
		// group newer versions sort first, so SeekGE lands on it (or past the
		// group when every version is newer than R).
		it.SeekGE(dataKey(raw, R))
		if !it.Valid() || !bytes.Equal(extractKey(it.Key()), raw) {
			belowGroup(it, raw)
			continue
		}
		val := it.Value()
		deleted := len(val) == 0 || val[0] == flagTomb
		var payload []byte
		if !deleted {
			payload = append([]byte(nil), val[1:]...)
		}
		if err := fn(raw, payload, deleted); err != nil {
			return err
		}
		belowGroup(it, raw)
	}
	return it.Error()
}

// belowGroup moves the iterator to the last key strictly below raw's version
// group. SeekLT(dataKey(raw, 0)) is not enough on its own: every version of
// raw sorts below that boundary, so it lands on raw's own newest version.
func belowGroup(it *pebble.Iterator, raw []byte) {
	it.SeekLT(dataKey(raw, 0))
	for it.Valid() && bytes.Equal(extractKey(it.Key()), raw) {
		it.Prev()
	}
}

// CompareAndSwap atomically applies op for key at rev IF the key's current
// value (as of the last committed revision) equals expected. When
// expectMissing is true the op applies only if the key does not exist.
// Returns false (and a nil error) when the comparison fails.
func (s *Store) CompareAndSwap(rev Revision, key, expected []byte, expectMissing bool, op Op) (bool, error) {
	if rev == 0 {
		return false, errors.New("kv: CAS requires an explicit revision")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, found, err := s.getLatest(key)
	if err != nil {
		return false, err
	}
	if expectMissing {
		if found {
			return false, nil
		}
	} else if !found || !bytes.Equal(cur, expected) {
		return false, nil
	}
	return true, s.applyLocked(rev, []Op{op})
}

// Lease is a time-bound handle used for expiration of key groups (groundwork:
// the lease registry is persisted in meta, but the key↔lease binding layer is
// the next step).
type Lease struct {
	id      uint64
	expires Revision // nanosecond timestamp as a plain value
	store   *Store
}

// GrantLease creates a lease with the given TTL (seconds as fractional).
// The expiry is persisted so it survives restarts; ids are issued from a
// monotonic counter so they are never reused, even after revocation.
func (s *Store) GrantLease(ttl float64) (*Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, _ := s.readMeta(metaLeaseSeq)
	var id uint64
	if len(seq) == 8 {
		id = binary.BigEndian.Uint64(seq)
	}
	id++
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	s.db.Set(metaKey(metaLeaseSeq), b[:], nil) // next id to issue
	exp := nowNanos() + uint64(ttl*1e9)
	binary.BigEndian.PutUint64(b[:], exp)
	if err := s.db.Set(append(metaKey(metaLeaseID), encodeUint64(id)...), b[:], pebble.NoSync); err != nil {
		return nil, err
	}
	return &Lease{id: id, expires: Revision(exp), store: s}, nil
}

// RevokeLease removes a lease (and in the future its bound keys).
func (s *Store) RevokeLease(id uint64) error {
	return s.db.Delete(append(metaKey(metaLeaseID), encodeUint64(id)...), pebble.NoSync)
}

// Expired reports whether the lease's TTL has elapsed.
func (l *Lease) Expired() bool { return nowNanos() > uint64(l.expires) }

func (s *Store) effective(rev Revision) Revision {
	if rev != 0 {
		return rev
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// getLatest returns the newest committed version of key. Caller holds s.mu.
func (s *Store) getLatest(key []byte) ([]byte, bool, error) {
	if s.last == 0 {
		return nil, false, nil
	}
	return s.Get(s.last, key)
}

func (s *Store) readMeta(name string) ([]byte, error) {
	v, closer, err := s.db.Get(metaKey(name))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, nil
}

func metaKey(name string) []byte {
	return append([]byte{metaTag}, name...)
}

// dataKey builds the physical key for one version.
func dataKey(key []byte, rev Revision) []byte {
	buf := make([]byte, 0, 1+len(key)+sepSize+suffixSize)
	buf = append(buf, dataTag)
	buf = append(buf, key...)
	buf = append(buf, 0)
	return append(buf, revSuffix(rev)...)
}

// dataUpper is the exclusive upper bound for all versions of key.
func dataUpper(key []byte) []byte {
	return dataUpperInto(nil, key)
}

// dataUpperInto writes the exclusive upper bound for all versions of key into
// buf (reused across iterations to avoid per-key allocations) and returns it.
func dataUpperInto(buf, key []byte) []byte {
	buf = append(buf, dataTag)
	buf = append(buf, key...)
	buf = append(buf, 0x01) // past the separator \x00
	return buf
}

func extractKey(dk []byte) []byte {
	return dk[1 : len(dk)-sepSize-suffixSize]
}

func revSuffix(rev Revision) []byte {
	var out [suffixSize]byte
	binary.BigEndian.PutUint64(out[:], ^uint64(rev))
	return out[:]
}

// nextKey returns the smallest byte key strictly greater than k, or nil when
// no such key exists (k is all 0xff).
func nextKey(k []byte) []byte {
	if k == nil {
		return nil
	}
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] != 0xff {
			n := append([]byte(nil), k[:i]...)
			n = append(n, k[i]+1)
			return n
		}
	}
	return nil
}

func encodeUint64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

var _ io.Closer = (*Store)(nil)

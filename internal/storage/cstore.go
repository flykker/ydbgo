package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sort"
	"sync"

	"ydbgo/internal/kv"
	sqlx "ydbgo/internal/sql"
)

// errStop is an internal sentinel to end a bounded scan early (used by
// ScanTopN); it is converted to a nil error by the caller.
var errStop = errors.New("stop scan")

// CSTORE is a columnar (OLAP) backend backed by internal/kv (MVCC). Row data
// is stored column-major so a projection over one column reads one contiguous
// range; this is the storage shape OLAP scans/aggregates want. Layout inside
// one kv.Store key space:
//
//	's' \x00 <table>                       schema definition
//	'r' \x00 <table> \x00 <pk>             row marker -> column count
//	'c' \x00 <table> \x00 <colIdx:4BE> \x00 <pk>   one cell of one column
//
// A row is an encoded (count + variants) blob split across its column cells.
// Reads reconstruct rows from their cells; column-granular scans can skip the
// cells of untouched columns entirely.
const (
	cstoreSchemaTag byte = 's'
	cstoreRowTag    byte = 'r'
	cstoreColTag    byte = 'c'
	cstoreCountTag  byte = 'n'
)

// cstoreCountKey is the live-row-count meta key of a table (8-byte BE int64,
// absent when 0). It lives in the table's own engine store so the count update
// is atomic with the row writes in the same kv.Apply.
func cstoreCountKey(table string) []byte {
	b := make([]byte, 0, 2+len(table))
	b = append(b, cstoreCountTag, 0x00)
	return append(b, table...)
}

func cstoreColKey(table string, colIdx int, pk []byte) []byte {
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], uint32(colIdx))
	b := make([]byte, 0, 4+len(table)+4+len(pk))
	b = append(b, cstoreColTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	b = append(b, idx[:]...)
	b = append(b, 0x00)
	return append(b, pk...)
}

func cstoreColPrefix(table string, colIdx int) []byte {
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], uint32(colIdx))
	b := make([]byte, 0, 4+len(table)+4)
	b = append(b, cstoreColTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	b = append(b, idx[:]...)
	b = append(b, 0x00)
	return b
}

func cstoreColBounds(table string, colIdx int) (lower, upper []byte) {
	lower = cstoreColPrefix(table, colIdx)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

// cstoreColAllBounds covers every column cell of a table (all colIdx).
func cstoreColAllBounds(table string) (lower, upper []byte) {
	lower = append([]byte{cstoreColTag, 0x00}, table...)
	lower = append(lower, 0x00)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

func cstoreRowKey(table string, pk []byte) []byte {
	b := make([]byte, 0, 3+len(table)+len(pk))
	b = append(b, cstoreRowTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return append(b, pk...)
}

func cstoreRowPrefix(table string) []byte {
	b := make([]byte, 0, 3+len(table))
	b = append(b, cstoreRowTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return b
}

func cstoreRowBounds(table string) (lower, upper []byte) {
	lower = cstoreRowPrefix(table)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

// cstoreStore is the ENGINE=CSTORE store binding.
type cstoreStore struct {
	st *kv.Store
}

func openCStore(dir string) (*cstoreStore, error) {
	s, err := kv.Open(filepath.Join(dir, "engine_cstore"))
	if err != nil {
		return nil, err
	}
	return &cstoreStore{st: s}, nil
}

func (s *cstoreStore) Close() error { return s.st.Close() }

// CompactLSM forces a full LSM compaction over the store's key space.
func (s *cstoreStore) CompactLSM() error { return s.st.CompactLSM() }

func (s *cstoreStore) begin() (storeTx, error) {
	return &cstoreTx{s: s, overlay: map[string]*pending{}}, nil
}

func (s *cstoreStore) view(fn func(tx storeTx) error) error {
	return fn(&cstoreTx{s: s})
}

// snapshot captures a point-in-time view pinned at the current committed
// revision; release it with rollback.
func (s *cstoreStore) snapshot() (storeTx, error) {
	return &cstoreTx{s: s, snap: s.st.Snapshot()}, nil
}

// cstoreTx buffers writes and flushes as one kv.Apply on commit, mirroring
// kvTx's overlay/read-your-writes semantics with the columnar key layout.
// When snap is set the tx is a read-only point-in-time view (raft FSM
// snapshot path).
type cstoreTx struct {
	s       *cstoreStore
	overlay map[string]*pending
	ops     []kv.Op
	snap    *kv.Snapshot
	counts  map[string]int64 // table -> live-row delta accumulated by this tx
}

func (t *cstoreTx) pkey(key []byte) string { return string(key) }

func (t *cstoreTx) put(key, value []byte) {
	k := t.pkey(key)
	t.overlay[k] = &pending{value: append([]byte(nil), value...)}
	t.ops = append(t.ops, kv.Op{Key: append([]byte(nil), key...), Value: value})
}

func (t *cstoreTx) del(key []byte) {
	k := t.pkey(key)
	t.overlay[k] = &pending{del: true}
	t.ops = append(t.ops, kv.Op{Key: append([]byte(nil), key...), Delete: true})
}

func (t *cstoreTx) get(key []byte) ([]byte, error) {
	if p, ok := t.overlay[t.pkey(key)]; ok {
		if p.del {
			return nil, nil
		}
		return p.value, nil
	}
	if t.snap != nil {
		v, ok, err := t.snap.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return v, nil
	}
	v, ok, err := t.s.st.Get(0, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return v, nil
}

// colCell point-reads one column cell at an exact pk (nil if absent/deleted).
func (t *cstoreTx) colCell(table string, colIdx int, pk []byte) ([]byte, error) {
	return t.get(cstoreColKey(table, colIdx, pk))
}

func (t *cstoreTx) each(lower, upper []byte, fn func(k, v []byte) error) error {
	// Fast path: no buffered writes, so the store's byte order is already the
	// result order and there is nothing to overlay/merge.
	if len(t.ops) == 0 {
		if t.snap != nil {
			return t.snap.Range(lower, upper, func(key, val []byte, deleted bool) error {
				if deleted {
					return nil
				}
				return fn(key, val)
			})
		}
		return t.s.st.Range(0, lower, upper, func(key, val []byte, deleted bool) error {
			if deleted {
				return nil
			}
			return fn(key, val)
		})
	}
	return t.eachOverlay(lower, upper, fn)
}

// eachNoCopy is each without the per-value copy from the store: the value is
// only valid during the callback, so fn must not retain it. The slow path
// (buffered writes) falls back to copying overlays.
func (t *cstoreTx) eachNoCopy(lower, upper []byte, fn func(k, v []byte) error) error {
	if len(t.ops) == 0 {
		if t.snap != nil {
			return t.snap.RangeNoCopy(lower, upper, func(key, val []byte, deleted bool) error {
				if deleted {
					return nil
				}
				return fn(key, val)
			})
		}
		return t.s.st.RangeNoCopy(0, lower, upper, func(key, val []byte, deleted bool) error {
			if deleted {
				return nil
			}
			return fn(key, val)
		})
	}
	return t.eachOverlay(lower, upper, fn)
}

func (t *cstoreTx) eachOverlay(lower, upper []byte, fn func(k, v []byte) error) error {
	ordered := []string{}
	vals := map[string][]byte{}
	t.s.st.Range(0, lower, upper, func(key, val []byte, deleted bool) error {
		if !deleted {
			ordered = append(ordered, t.pkey(key))
			vals[t.pkey(key)] = append([]byte(nil), val...)
		}
		return nil
	})
	for _, op := range t.ops {
		k := t.pkey(op.Key)
		if bytes.Compare(op.Key, lower) < 0 || bytes.Compare(op.Key, upper) >= 0 {
			continue
		}
		if op.Delete {
			if _, was := vals[k]; was {
				delete(vals, k)
			}
			continue
		}
		if _, was := vals[k]; !was {
			ordered = append(ordered, k)
		}
		vals[k] = op.Value
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		if err := fn([]byte(k), vals[k]); err != nil {
			return err
		}
	}
	return nil
}

func (t *cstoreTx) schemaPut(name string, def []byte) error {
	t.put(kvSchemaKey(name), def)
	return nil
}
func (t *cstoreTx) schemaGet(name string) ([]byte, error) { return t.get(kvSchemaKey(name)) }
func (t *cstoreTx) schemaDelete(name string) error        { t.del(kvSchemaKey(name)); return nil }
func (t *cstoreTx) schemaNames() ([]string, error) {
	names := []string{}
	lower, upper := kvSchemaBounds()
	err := t.each(lower, upper, func(k, _ []byte) error {
		names = append(names, string(k[len(lower):]))
		return nil
	})
	return names, err
}

// splitRow decodes an encoded row (count + variants) into its per-column cell
// blobs (each a variant in the codec's format).
func splitRow(encoded []byte) (int, [][]byte, error) {
	r := makeReader(encoded)
	n := int(r.Var())
	cells := make([][]byte, n)
	// re-encode each variant into its own blob using the builder
	for i := 0; i < n; i++ {
		v := r.Variant()
		if r.err != nil {
			return 0, nil, r.err
		}
		b := makeBuilder()
		b.Variant(v)
		cells[i] = b.Bytes()
	}
	return n, cells, nil
}

// joinRow reassembles the encoded row blob from its column cells.
func joinRow(cells [][]byte) []byte {
	b := makeBuilder()
	b.Var(int64(len(cells)))
	for _, c := range cells {
		b.buf = append(b.buf, c...)
	}
	return b.Bytes()
}

func (t *cstoreTx) rowPut(table string, key []byte, val []byte) error {
	_, cells, err := splitRow(val)
	if err != nil {
		return err
	}
	return t.rowPutCells(table, key, cells)
}

func (t *cstoreTx) rowPutCells(table string, key []byte, cells [][]byte) error {
	return t.rowPutCellsCheckCount(table, key, cells, true)
}

// rowPutCellsNoCount skips the existence probe used to keep the live-row count
// in sync; callers that know every key exists (the columnar UPDATE batch) use
// it to avoid one point read per row.
func (t *cstoreTx) rowPutCellsNoCount(table string, key []byte, cells [][]byte) error {
	return t.rowPutCellsCheckCount(table, key, cells, false)
}

func (t *cstoreTx) rowPutCellsCheckCount(table string, key []byte, cells [][]byte, check bool) error {
	n := len(cells)
	// A row that becomes live raises the table's live count; an overwrite of
	// an already-live row does not. The existence read goes through the
	// overlay so repeated writes of the same pk in one tx are exact.
	if check {
		if was, err := t.get(cstoreRowKey(table, key)); err != nil {
			return err
		} else if len(was) == 0 {
			t.count(table, 1)
		}
	}
	var cnt [8]byte
	binary.BigEndian.PutUint64(cnt[:], uint64(n))
	t.put(cstoreRowKey(table, key), cnt[:])
	for i, c := range cells {
		t.put(cstoreColKey(table, i, key), c)
	}
	return nil
}

// count adjusts the live-row delta of a table. Lazily allocates the map so
// read-only txs (view/snapshot) stay allocation-free.
func (t *cstoreTx) count(table string, delta int64) {
	if t.counts == nil {
		t.counts = map[string]int64{}
	}
	t.counts[table] += delta
}

// committedCount returns the store's committed live-row count of a table (0
// when unset), ignoring this tx's own uncommitted deltas.
func (t *cstoreTx) committedCount(table string) (int64, error) {
	raw, err := t.get(cstoreCountKey(table))
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

// countFor returns the live-row count of a table as seen from this tx: the
// committed value plus the tx's own uncommitted deltas (read-your-writes for
// reads that route through an in-progress group-commit batch).
func (t *cstoreTx) countFor(table string) (int64, error) {
	base, err := t.committedCount(table)
	if err != nil {
		return 0, err
	}
	return base + t.counts[table], nil
}

func (t *cstoreTx) rowGet(table string, key []byte) ([]byte, error) {
	cntRaw, err := t.get(cstoreRowKey(table, key))
	if err != nil || len(cntRaw) == 0 {
		return nil, err
	}
	n := int(binary.BigEndian.Uint64(cntRaw))
	cells := make([][]byte, n)
	for i := 0; i < n; i++ {
		c, err := t.get(cstoreColKey(table, i, key))
		if err != nil {
			return nil, err
		}
		if len(c) == 0 {
			return nil, nil // partially deleted row
		}
		cells[i] = c
	}
	return joinRow(cells), nil
}

func (t *cstoreTx) rowDelete(table string, key []byte) error {
	cntRaw, err := t.get(cstoreRowKey(table, key))
	if err != nil {
		return err
	}
	t.del(cstoreRowKey(table, key))
	if len(cntRaw) == 0 {
		return nil
	}
	if t.counts == nil {
		t.counts = map[string]int64{}
	}
	t.counts[table]--
	n := int(binary.BigEndian.Uint64(cntRaw))
	for i := 0; i < n; i++ {
		t.del(cstoreColKey(table, i, key))
	}
	return nil
}

func (t *cstoreTx) rowEach(table string, fn func(k, v []byte) error) error {
	lower, upper := cstoreRowBounds(table)
	pl := len(lower)
	return t.each(lower, upper, func(k, _ []byte) error {
		encoded, err := t.rowGet(table, k[pl:])
		if err != nil || encoded == nil {
			return err
		}
		return fn(k[pl:], encoded)
	})
}

// colRowKeys iterates the primary keys of a table in sorted order, reading
// only row markers (no cell reconstruction).
func (t *cstoreTx) colRowKeys(table string, fn func(pk []byte) error) error {
	return t.colRowKeysRange(table, nil, nil, fn)
}

// colRowKeysRange is colRowKeys restricted to pks within [plLower, plUpper)
// (encoded pk byte bounds; nil = unbounded on that side).
func (t *cstoreTx) colRowKeysRange(table string, plLower, plUpper []byte, fn func(pk []byte) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range (e.g. pk >= hi AND pk < lo) matches nothing
	}
	prefix := cstoreRowPrefix(table)
	lower := prefix
	if len(plLower) > 0 {
		lower = append(append([]byte(nil), prefix...), plLower...)
	}
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	if len(plUpper) > 0 {
		upper = append(prefix, plUpper...)
	}
	pl := len(prefix)
	// The row value is not needed (only the presence of the pk), so use the
	// zero-copy scan: the callback must not retain pk.
	return t.eachNoCopy(lower, upper, func(k, _ []byte) error {
		return fn(k[pl:])
	})
}

// colRowCountRange counts row markers in [plLower, plUpper) without invoking a
// per-row callback, using the kv-level bulk count (Row markers are written
// with the kv layer, so distinct row keys are counted once each).
func (t *cstoreTx) colRowCountRange(table string, plLower, plUpper []byte) (int64, error) {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return 0, nil
	}
	prefix := cstoreRowPrefix(table)
	lower := prefix
	if len(plLower) > 0 {
		lower = append(append([]byte(nil), prefix...), plLower...)
	}
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	if len(plUpper) > 0 {
		upper = append(prefix, plUpper...)
	}
	if t.snap != nil {
		return t.snap.RangeCount(lower, upper)
	}
	if len(t.ops) == 0 {
		return t.s.st.RangeCount(0, lower, upper)
	}
	// Buffered writes overlay the store: fall back to the merging scan.
	var n int64
	if err := t.eachNoCopy(lower, upper, func(k, _ []byte) error {
		n++
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// colEach iterates the cells of one column, delivering each pk and its cell
// blob (a single variant).
func (t *cstoreTx) colEach(table string, colIdx int, fn func(pk, cell []byte) error) error {
	return t.colEachRange(table, colIdx, nil, nil, fn)
}

// colEachRange is colEach restricted to pks within [plLower, plUpper) (encoded
// pk byte bounds; nil = unbounded on that side).
func (t *cstoreTx) colEachRange(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error {
	return t.colEachRangeIt(table, colIdx, plLower, plUpper, false, fn)
}

// colEachRangeNoCopy is colEachRange without the per-cell copy from the store:
// cell is only valid during the callback, so fn must not retain it.
func (t *cstoreTx) colEachRangeNoCopy(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error {
	return t.colEachRangeIt(table, colIdx, plLower, plUpper, true, fn)
}

func (t *cstoreTx) colEachRangeIt(table string, colIdx int, plLower, plUpper []byte, noCopy bool, fn func(pk, cell []byte) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range matches nothing
	}
	prefix := cstoreColPrefix(table, colIdx)
	lower := prefix
	if len(plLower) > 0 {
		lower = append(append([]byte(nil), prefix...), plLower...)
	}
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	if len(plUpper) > 0 {
		upper = append(prefix, plUpper...)
	}
	pl := len(prefix)
	if noCopy {
		return t.eachNoCopy(lower, upper, func(k, v []byte) error {
			return fn(k[pl:], v)
		})
	}
	return t.each(lower, upper, func(k, v []byte) error {
		return fn(k[pl:], v)
	})
}

// numVec is a bulk-decoded numeric column: dense arrays indexed by cell
// position (0..count-1) plus a null bitset. It backs the vectorized scan path:
// the column range is scanned once (no per-cell reader/closure decode) and
// aggregates run over the dense arrays afterwards.
type numVec struct {
	typ      sqlType
	count    int
	ints     []int64
	floats   []float64
	nulls    []uint64 // bit p set => cell p is NULL
	borrowed bool     // slices reference part caches; putNumVec must not recycle
}

// numVecPool recycles the 8-16MB dense column buffers between queries. Large
// slices are mmap'd and returned via madvise on free (visible in GROUP BY
// profiles as runtime.madvise/scavenge), so reusing the backing arrays keeps
// hot GROUP/SUM loops off the allocator. The pool only reuses an instance
// when a caller puts it back; dropped puts only cost reuse, never correctness.
var numVecPool = sync.Pool{New: func() any { return &numVec{} }}

func poolNumVec() *numVec {
	v := numVecPool.Get().(*numVec)
	v.ints, v.floats, v.nulls = v.ints[:0], v.floats[:0], v.nulls[:0]
	v.count = 0
	v.borrowed = false
	return v
}

func putNumVec(v *numVec) {
	if v.borrowed {
		return // borrowed slices alias part caches; never recycle them
	}
	numVecPool.Put(v)
}

func (v *numVec) nullAt(p int) bool {
	if p>>6 >= len(v.nulls) {
		return false
	}
	return v.nulls[p>>6]&(1<<(uint(p)&63)) != 0
}

func (v *numVec) setNull(p int) {
	for len(v.nulls) <= p>>6 {
		v.nulls = append(v.nulls, 0)
	}
	v.nulls[p>>6] |= 1 << (uint(p) & 63)
}

// colDecodeNumeric vectorizes the decode of one numeric column range into a
// dense numVec. Numeric cells are decoded inline; a cell with an unexpected
// shape falls back to the generic reader (legacy data).
func (t *cstoreTx) colDecodeNumeric(table string, colIdx int, typ sqlType, plLower, plUpper []byte) (*numVec, error) {
	v := &numVec{typ: typ}
	n := 0
	if c, err := t.countFor(table); err == nil && c > 0 {
		n = int(c)
	}
	if typ == tFloat {
		v.floats = make([]float64, 0, n)
		err := t.colEachRangeNoCopy(table, colIdx, plLower, plUpper, func(_, cell []byte) error {
			_, f, null, ok := decodeNumericCell(cell, tFloat)
			if !ok {
				val := makeReader(cell).Variant()
				if val.null {
					v.floats = append(v.floats, 0)
					v.setNull(v.count)
				} else {
					v.floats = append(v.floats, val.f)
				}
			} else if null {
				v.floats = append(v.floats, 0)
				v.setNull(v.count)
			} else {
				v.floats = append(v.floats, f)
			}
			v.count++
			return nil
		})
		return v, err
	}
	v.ints = make([]int64, 0, n)
	err := t.colEachRangeNoCopy(table, colIdx, plLower, plUpper, func(_, cell []byte) error {
		i, _, null, ok := decodeNumericCell(cell, typ)
		if !ok {
			val := makeReader(cell).Variant()
			if val.null {
				v.ints = append(v.ints, 0)
				v.setNull(v.count)
			} else {
				v.ints = append(v.ints, val.i)
			}
		} else if null {
			v.ints = append(v.ints, 0)
			v.setNull(v.count)
		} else {
			v.ints = append(v.ints, i)
		}
		v.count++
		return nil
	})
	return v, err
}

// colDecodeNumericFiltered is colDecodeNumeric; the row-store format keeps no
// zone maps, so there is nothing to prune.
func (t *cstoreTx) colDecodeNumericFiltered(table string, colIdx int, typ sqlType, pred *sqlx.ColumnFilter, plLower, plUpper []byte) (*numVec, error) {
	return t.colDecodeNumeric(table, colIdx, typ, plLower, plUpper)
}

// eachDesc is each iterating in reverse byte order.
func (t *cstoreTx) eachDesc(lower, upper []byte, fn func(k, v []byte) error) error {
	if len(t.ops) == 0 {
		return t.s.st.RangeDesc(0, lower, upper, func(key, val []byte, deleted bool) error {
			if deleted {
				return nil
			}
			return fn(key, val)
		})
	}
	ordered := []string{}
	vals := map[string][]byte{}
	t.s.st.Range(0, lower, upper, func(key, val []byte, deleted bool) error {
		if !deleted {
			ordered = append(ordered, t.pkey(key))
			vals[t.pkey(key)] = append([]byte(nil), val...)
		}
		return nil
	})
	for _, op := range t.ops {
		k := t.pkey(op.Key)
		if bytes.Compare(op.Key, lower) < 0 || bytes.Compare(op.Key, upper) >= 0 {
			continue
		}
		if op.Delete {
			if _, was := vals[k]; was {
				delete(vals, k)
			}
			continue
		}
		if _, was := vals[k]; !was {
			ordered = append(ordered, k)
		}
		vals[k] = op.Value
	}
	sort.Strings(ordered)
	for i := len(ordered) - 1; i >= 0; i-- {
		if err := fn([]byte(ordered[i]), vals[ordered[i]]); err != nil {
			return err
		}
	}
	return nil
}

// colRowKeysRangeDesc is colRowKeysRange iterating in reverse byte order.
func (t *cstoreTx) colRowKeysRangeDesc(table string, plLower, plUpper []byte, fn func(pk []byte) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range matches nothing
	}
	prefix := cstoreRowPrefix(table)
	lower := prefix
	if len(plLower) > 0 {
		lower = append(append([]byte(nil), prefix...), plLower...)
	}
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	if len(plUpper) > 0 {
		upper = append(prefix, plUpper...)
	}
	pl := len(prefix)
	return t.eachDesc(lower, upper, func(k, _ []byte) error {
		return fn(k[pl:])
	})
}

func (t *cstoreTx) rowDeleteAll(table string) error {
	lower, upper := cstoreRowBounds(table)
	var rowKeys [][]byte
	t.each(lower, upper, func(k, _ []byte) error {
		rowKeys = append(rowKeys, append([]byte(nil), k...))
		return nil
	})
	for _, k := range rowKeys {
		if err := t.rowDelete(table, k[len(lower):]); err != nil {
			return err
		}
	}
	// drop any orphan column cells (no marker, e.g. after partial writes)
	cl, cu := cstoreColAllBounds(table)
	var colKeys [][]byte
	t.each(cl, cu, func(k, _ []byte) error {
		colKeys = append(colKeys, append([]byte(nil), k...))
		return nil
	})
	for _, k := range colKeys {
		t.del(k)
	}
	return nil
}

func (t *cstoreTx) dropTable(name string) error {
	if err := t.schemaDelete(name); err != nil {
		return err
	}
	return t.rowDeleteAll(name)
}

func (t *cstoreTx) commit() error {
	if len(t.ops) == 0 && len(t.counts) == 0 {
		return nil
	}
	// Fold live-row deltas into the committed counter: the counter key is not
	// in the overlay, so committedCount reads the pre-tx committed value,
	// making the update atomic with the row writes in one kv.Apply.
	for table, delta := range t.counts {
		if delta == 0 {
			continue
		}
		base, err := t.committedCount(table)
		if err != nil {
			return err
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(base+delta))
		t.ops = append(t.ops, kv.Op{Key: cstoreCountKey(table), Value: buf[:]})
	}
	if len(t.ops) == 0 {
		return nil
	}
	if err := t.s.st.Apply(0, t.ops); err != nil {
		return err
	}
	t.ops = nil
	t.overlay = map[string]*pending{}
	t.counts = nil
	return nil
}

func (t *cstoreTx) rollback() error {
	t.ops = nil
	t.overlay = map[string]*pending{}
	t.counts = nil
	if t.snap != nil {
		t.snap.Close()
		t.snap = nil
	}
	return nil
}

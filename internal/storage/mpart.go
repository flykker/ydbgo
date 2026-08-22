package storage

// Native columnar engine (ENGINE=CSTORE2). Data is organised as a small
// log-structured merge tree of immutable, sorted-by-PK parts, one file per
// column (LZ4-compressed frames), with a PK bloom filter for cheap point
// lookups. In-flight writes are buffered in a mem part (append-only, one
// entry per PK) and flushed to a new part once the mem part crosses a row
// threshold. Reads merge mem + parts + the in-transaction overlay in PK
// order; tombstones and updates are resolved by newest-writer-wins during the
// merge. Like the rest of the state cache the store is not durable by itself:
// the raft log owns durability, so commits skip fsync and part files may be
// discarded on restart (raft replays them).

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	sqlx "ydbgo/internal/sql"
)

// mpartFlushThreshold is the mem-part row count that triggers a flush to a
// new immutable part.
const mpartFlushThreshold = 65536

// mpartMergeMinParts is how many on-disk parts a table must accumulate before
// the idle flusher merges overlapping ones. Partial-merge UPDATEs rewrite the
// same window repeatedly; each flush adds one part that overlaps the base, and
// eager merging would rewrite the whole table after every statement. Reads stay
// exact without merges (column-wise inheritance + the covering-newest-source
// fast path), so merging is deferred until the part count justifies the
// full-table rewrite (ClickHouse defers background merges the same way).
const mpartMergeMinParts = 10

// mpartMergeChunk is the maximum part size compactTable/mergeIdle rewrite a
// table into. Merges are allowed to produce far larger parts than the write
// flush threshold: after a merge the parts are disjoint, so a full-table
// aggregate reads one big dense column in one file-open + LZ4 pass instead of
// hundreds of small ones. This is the ClickHouse merge-tree answer to part
// proliferation (its background merges coalesce parts into ~150MB ones).
const mpartMergeChunk = 1 << 20

// mpartIdleFlushInterval is how long a mem part must receive no writes before
// the background flusher turns it into a part. Mirrors ClickHouse's async
// insert busy-timeout (AsynchronousInsertQueue): a small trailing mem tail
// (the rows written after the last size-triggered flush) must not stay in the
// mem part forever, because every read would then re-sort and re-encode it.
const mpartIdleFlushInterval = 500 * time.Millisecond

// mpartIdleFlushMinRows is the minimum mem-part size for a background idle
// flush. Trickle workloads (a few rows every so often) keep their rows in the
// mem part until a real flush threshold or Close; only a non-trivial idle tail
// is worth turning into a part.
const mpartIdleFlushMinRows = 4096

const (
	mpartMagic       = "MPART1"
	mpartVersion     = 4
	mpartDirName     = "engine_mpart"
	mpartPartsDir    = "parts"
	mpartGranuleRows = 65536

	mpartIdxMagic = "MPIDX1"
	mpartIdxVer   = 3
)

// Column format tags stored per column in meta.bin. colFmtLegacy means the
// column file holds the v1/v2 LZ4-frames layout (uvarint-length-prefixed cell
// blobs). colFmtDensePrefix marks a dense fixed-width numeric column: the file
// is LZ4 of [uvarint nrows][null bitmap ceil(nrows/8)][nrows*8 big-endian
// int64 values], so aggregates decode one typed array instead of parsing
// 1M varint cells (ClickHouse wide-part columns). The dense type byte is the
// tag's least-significant payload; readers recover sqlType as tag - base.
const colFmtLegacy byte = 0
const colFmtDenseBase byte = 1

func colFmtDense(t sqlType) byte { return colFmtDenseBase + byte(t) }
func colFmtType(f byte) (sqlType, bool) {
	return sqlType(int(f) - int(colFmtDenseBase)), f >= colFmtDenseBase
}

// memRow is a single buffered row: the encoded PK plus one encoded cell blob
// per table column. memRow is immutable once created (a write replaces the
// map entry, a delete replaces it with a tombstone), which lets snapshots
// share rows without copying values.
type memRow struct {
	pk    []byte
	cells [][]byte
	del   bool
}

type memPart struct {
	rows map[string]*memRow

	// Sorted-view cache. memRow is immutable once created and rows are only
	// added by commit() (under the store lock), so the decoded view built from
	// the current rows stays valid until the next commit bumps gen. Repeated
	// reads of a stable mem tail (the trailing insert buffer before a flush)
	// therefore avoid re-sorting and re-allocating per query — the ClickHouse
	// "decode once, read many" pattern applied to the in-memory part.
	gen        uint64 // bumped on every commit that adds rows
	cacheGen   uint64 // gen the cached view was built at
	cacheRows  []*memRow
	cachePks   [][]byte
	cacheDels  []bool
	cacheCells map[int][][]byte // lazily built per column index
	cacheFulls [][][]byte

	// hint carries the rows in insertion order when a writer knows it inserted
	// each key exactly once in ascending PK order (the columnar UPDATE batch:
	// its rows come sorted from the scan). len(hint) == len(rows) is the
	// validity check — any duplicate key or unrelated mutation breaks the
	// length match and the hint is ignored. Lets flush/ensureCached skip the
	// O(n log n) sort.
	hint []*memRow
}

// mpartGranule is one sparse-index entry: the granule's PK window (pkMax) and
// the byte offsets of the granule's independently-compressed LZ4 block within
// pk.bin, each col_N.bin, and del.bin. Granules let point reads and ORDER BY
// ... LIMIT decode only the block that can contain the key, instead of the
// whole part (ClickHouse primary/granule index).
type mpartGranule struct {
	pkMax  []byte
	pkOff  int64
	pkLen  int64
	pkRaw  int64 // uncompressed size of the granule's pk.bin block (0 = frame)
	delOff int64
	delLen int64
	delRaw int64
	colOff []int64 // per column: offset of this granule's block in col_N.bin
	colLen []int64
	colRaw []int64 // per column: uncompressed size (0 = frame)
	// zoneMin/zoneMax are per-dense-column value bounds over the granule's rows
	// (idx ver 3; nil for legacy/framed columns). They let filtered scans skip
	// granules whose values cannot match a numeric WHERE predicate (ClickHouse
	// zone maps / secondary minmax index analog).
	zoneMin []int64
	zoneMax []int64
}

// mpart is an immutable part on disk: pks sorted ascending, one column file
// per table column, meta.bin written last as the seal marker. pkMin/pkMax and
// the bloom filter let readers skip parts that cannot contain a range or key.
// Parts written at format v4 are granule-aligned: pk.bin, col_N.bin and
// del.bin hold one independently-LZ4-compressed block per mpartGranuleRows
// rows, and idx.bin (the sparse index) records each granule's PK window plus
// per-file block offsets. v2/v3 parts (no idx.bin) read back as a single
// block.
type mpart struct {
	table    string
	ncols    int
	rowcount int
	seq      int // monotonic creation order; newer parts win on PK overlap
	pkMin    []byte
	pkMax    []byte
	dir      string
	crcs     []uint32
	bloom    *mpartBloom

	// granules is the sparse index (v4 parts). Nil/empty for v2/v3 parts.
	granules []mpartGranule

	// Parts are immutable, so the decoded PK list is safe to memoize: point
	// lookups hit the bloom filter for every key, and without this cache each
	// bloom hit would re-read + re-decompress the whole part file.
	pksOnce sync.Once
	pks     [][]byte
	pksErr  error

	// dels[i] records whether row i is a tombstone (a flushed DELETE). It is
	// stored as a bitmap in del.bin (immutable, so also memoized).
	delsOnce sync.Once
	dels     []bool
	delsErr  error

	// cols caches decoded per-column cells ([]byte per row), one entry per
	// table column. Parts are immutable, so like pks they are safe to memoize;
	// this is the ClickHouse UncompressedCache analog for repeated columnar
	// reads (point gets, ORDER BY ... LIMIT point-reads, repeated aggregates).
	colsOnce []sync.Once
	cols     [][][]byte
	colsErr  []error

	// colFmts records, per column, whether the column file is stored in the
	// dense fixed-width numeric layout (colFmtDense+type) or the legacy frames
	// layout (colFmtLegacy). Written in meta.bin (format v3); v1/v2 parts read
	// back as all-legacy.
	colFmts []byte

	// dense caches the decoded fixed-width numeric column values (one int64
	// per row) plus a null bitmap, parallel to cols. Only used for dense
	// columns; populated once per part because parts are immutable.
	denseOnce  []sync.Once
	denseVals  [][]int64
	denseNulls [][]uint64
	denseErr   []error

	// Per-granule decode caches (v4 parts). Granule blocks are immutable, so
	// repeated point reads / ORDER BY ... LIMIT / UPDATE row reads on the same
	// granule decode each block only once. gCols is [colIdx][g][row].
	gPksOnce  []sync.Once
	gPks      [][][]byte
	gPksErr   []error
	gDelsOnce []sync.Once
	gDels     [][]bool
	gDelsErr  []error
	gColsOnce [][]sync.Once
	gCols     [][][][]byte
	gColsErr  [][]error

	// gDense caches per-granule dense numeric values ([colIdx][g]), decoded
	// lazily by zone-map-filtered scans that only touch the granules whose
	// value bounds can satisfy the predicate.
	gDenseOnce  [][]sync.Once
	gDense      [][][]int64
	gDenseNulls [][][]uint64
	gDenseErr   [][]error

	// preloadOnce guards the background eager decode of every dense column
	// (loadColDense), launched right after an idle flush/merge so the first
	// SUM/GROUP query finds a warm decode cache instead of paying the cold
	// LZ4 pass inline.
	preloadOnce sync.Once
}

// inRange reports whether the part's PK span intersects [lo, hi) (nil = open).
func (p *mpart) inRange(lo, hi []byte) bool {
	if lo != nil && bytes.Compare(p.pkMax, lo) < 0 {
		return false
	}
	if hi != nil && bytes.Compare(p.pkMin, hi) >= 0 {
		return false
	}
	return true
}

func (p *mpart) colPath(col int) string {
	return filepath.Join(p.dir, fmt.Sprintf("col_%d.bin", col))
}

// mpartStore is the backend implementing store for ENGINE=CSTORE2.
type mpartStore struct {
	dir string // <engine dir>/engine_mpart

	mu     sync.Mutex
	mem    map[string]*memPart
	parts  map[string][]*mpart
	counts map[string]int64

	nextID int

	// snapRefs counts live snapshot() transactions. While non-zero, flushed
	// away part files are only retired (renamed to trash) and deleted once
	// every snapshot has finished reading them.
	snapRefs int
	trash    []string

	// lastWrite records, per table, the time the mem part last received a
	// write. The background idle flusher uses it to turn a stable trailing
	// mem tail into a part (ClickHouse async-insert busy-timeout analog).
	lastWrite map[string]time.Time
	stopFlush chan struct{}
	flushDone chan struct{}

	// flushCh signals the background flusher to materialize a full mem part
	// into an immutable on-disk part. A mem part that crosses the size
	// threshold during a raft commit must NOT be flushed inline (that would
	// stall the FSM goroutine on LZ4 + file writes); instead the commit swaps
	// in a fresh mem part and hands the full one to the flusher worker.
	flushCh chan flushJob

	// preloadCh queues freshly flushed/merged parts whose dense columns should
	// be decoded in the background before the first query touches them. The
	// buffer is bounded; when full, the part is simply skipped (the query path
	// still decodes lazily).
	preloadCh chan *mpart
}

// flushJob is one full mem part handed to the background flusher.
type flushJob struct {
	tbl  string
	rows []*memRow
	id   int
}

// openMpart opens (creating if needed) an mpart store rooted at dir.
func openMpart(dir string) (*mpartStore, error) {
	root := filepath.Join(dir, mpartDirName)
	parts := filepath.Join(root, mpartPartsDir)
	if err := os.MkdirAll(parts, 0o755); err != nil {
		return nil, err
	}
	s := &mpartStore{
		dir:       root,
		mem:       map[string]*memPart{},
		parts:     map[string][]*mpart{},
		counts:    map[string]int64{},
		lastWrite: map[string]time.Time{},
		stopFlush: make(chan struct{}),
		flushDone: make(chan struct{}),
		flushCh:   make(chan flushJob, 8),
		preloadCh: make(chan *mpart, 16),
	}
	idMax := 0
	entries, err := os.ReadDir(parts)
	if err != nil {
		return nil, err
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		dir := filepath.Join(parts, ent.Name())
		p, err := readMpartMeta(dir)
		if err != nil {
			// Incomplete part (crash mid-flush): discard, raft replays its rows.
			os.RemoveAll(dir)
			continue
		}
		if n, err := fmt.Sscanf(ent.Name(), "p%08d", &s.nextID); err == nil && n == 1 {
			if s.nextID >= idMax {
				idMax = s.nextID
			}
			p.seq = s.nextID
		}
		s.parts[p.table] = append(s.parts[p.table], p)
	}
	for _, ps := range s.parts {
		sortPartsBySeq(ps)
	}
	// Live counts cannot be derived by summing part rowcounts: parts overlap
	// in PK space (an update rewrites a pk that already lives in an older part)
	// and flushes may contain tombstone rows. Recompute each table's count by
	// merging its parts.
	for tbl := range s.parts {
		n, err := s.liveCountLocked(tbl)
		if err != nil {
			return nil, err
		}
		s.counts[tbl] = n
	}
	s.nextID = idMax + 1
	s.startIdleFlusher()
	return s, nil
}

// startIdleFlusher launches the background goroutine that turns a stable
// trailing mem tail into an immutable part and merges overlapping parts
// (ClickHouse background-merge analog). It observes the committed mem/parts
// (only touched under s.mu), so it never races an in-flight commit. It exits
// on Close. It also drains flushCh, which carries full mem parts detached by
// a commit that crossed the mem size threshold (async threshold flush).
func (s *mpartStore) startIdleFlusher() {
	noMerge := os.Getenv("YDBGO_NO_MERGE") == "1"
	go func() {
		t := time.NewTicker(mpartIdleFlushInterval / 2)
		defer t.Stop()
		defer close(s.flushDone)
		for {
			select {
			case <-s.stopFlush:
				return
			case j := <-s.flushCh:
				s.flushJobAsync(j)
			case now := <-t.C:
				s.flushIdle(now)
				if !noMerge {
					s.mergeIdle(now)
				}
			}
		}
	}()
	// Background dense-column prefetch: freshly flushed/merged parts are
	// decoded eagerly so the first aggregate query sees a warm cache. A
	// separate goroutine keeps this from adding latency to the flusher tick.
	go func() {
		for {
			select {
			case <-s.stopFlush:
				return
			case p := <-s.preloadCh:
				p.preloadDense()
			}
		}
	}()
}

// partsOverlap reports whether the table's on-disk parts overlap pairwise in
// PK window. Concurrent/interleaved writers (the bench's -c 8, or interleaved
// raft applies) produce parts whose sorted PK windows overlap even when no
// actual row is duplicated; the disjoint fast path then bails and reads pay a
// k-way merge. ClickHouse merges such parts in the background; so do we.
func (s *mpartStore) partsOverlap(tbl string) bool {
	ps := s.parts[tbl]
	if len(ps) < 2 {
		return false
	}
	sorted := append([]*mpart(nil), ps...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].pkMin, sorted[j].pkMin) < 0 })
	for k := 0; k < len(sorted)-1; k++ {
		if bytes.Compare(sorted[k].pkMax, sorted[k+1].pkMin) >= 0 {
			return true
		}
	}
	return false
}

// mergeIdle merges every table whose on-disk parts overlap in PK window and
// which has received no writes for mpartIdleFlushInterval. The rewritten parts
// are disjoint by construction, so subsequent aggregate reads take the dense
// fast path instead of a k-way heap merge.
func (s *mpartStore) mergeIdle(now time.Time) {
	s.mu.Lock()
	var targets []string
	for tbl := range s.parts {
		last, ok := s.lastWrite[tbl]
		if !ok {
			continue
		}
		if now.Sub(last) < mpartIdleFlushInterval {
			continue
		}
		if len(s.parts[tbl]) >= mpartMergeMinParts && s.partsOverlap(tbl) {
			targets = append(targets, tbl)
		}
	}
	s.mu.Unlock()
	for _, tbl := range targets {
		if _, err := s.compactTable(tbl); err != nil {
			// Best-effort: reads stay correct (parts still overlap; the merge
			// retries next tick). The store has no logger, so this is surfaced
			// only via the returned error.
			_ = err
			continue
		}
		s.queuePreload(tbl)
	}
}

// queuePreload schedules the table's dense columns for background decode. Only
// parts with at least mpartIdleFlushMinRows rows are worth the prefetch; small
// trailing parts decode trivially on demand.
func (s *mpartStore) queuePreload(tbl string) {
	s.mu.Lock()
	parts := append([]*mpart(nil), s.parts[tbl]...)
	s.mu.Unlock()
	for _, p := range parts {
		if p == nil || p.rowcount < mpartIdleFlushMinRows {
			continue
		}
		select {
		case s.preloadCh <- p:
		default:
			// Channel full: the prefetch worker is busy; skip rather than
			// block the flusher.
		}
	}
}

// flushIdle flushes every table whose mem part has not received a write for
// mpartIdleFlushInterval and holds at least mpartIdleFlushMinRows rows.
// The actual part write happens outside the store lock so a large trailing
// mem part (bulk load) never stalls concurrent reads on s.mu.
func (s *mpartStore) flushIdle(now time.Time) {
	var jobs []flushJob
	s.mu.Lock()
	for tbl, mp := range s.mem {
		if mp == nil || len(mp.rows) < mpartIdleFlushMinRows {
			continue
		}
		last, ok := s.lastWrite[tbl]
		if !ok {
			continue
		}
		if now.Sub(last) < mpartIdleFlushInterval {
			continue
		}
		rows := make([]*memRow, 0, len(mp.rows))
		for _, r := range mp.rows {
			rows = append(rows, r)
		}
		s.nextID++
		s.mem[tbl] = &memPart{rows: map[string]*memRow{}}
		s.lastWrite[tbl] = time.Now()
		jobs = append(jobs, flushJob{tbl: tbl, rows: rows, id: s.nextID})
	}
	s.mu.Unlock()
	for _, j := range jobs {
		s.flushJobAsync(j)
	}
}

// flushJobAsync writes a mem part (still resident in s.mem) to an immutable
// part, outside the store lock so a large part never stalls reads or a raft
// commit that is waiting on s.mu. Runs on the flusher worker. Rows stay
// visible in mem until the part is sealed; the worker then removes from mem
// only the row pointers it actually wrote (newer overwrites of the same PK
// keep their pointer and stay in mem, winning the read merge), and publishes
// the part. On write error the rows remain in mem untouched.
func (s *mpartStore) flushJobAsync(j flushJob) {
	sortMemRows(j.rows)
	p, err := s.writePart(j.tbl, j.rows, j.id)
	if err != nil {
		return
	}
	s.mu.Lock()
	for _, r := range j.rows {
		if cur, ok := s.mem[j.tbl]; ok && cur != nil {
			if cur.rows[string(r.pk)] == r {
				delete(cur.rows, string(r.pk))
			}
		}
	}
	s.parts[j.tbl] = append(s.parts[j.tbl], p)
	sortPartsBySeq(s.parts[j.tbl])
	s.mu.Unlock()
	s.queuePreload(j.tbl)
}

// liveCountLocked counts the distinct live rows of tbl across its on-disk
// parts (caller holds no lock; it does a scratch read-only merge).
func (s *mpartStore) liveCountLocked(tbl string) (int64, error) {
	t := &mpartTx{s: s, overlay: map[string]*memPart{}}
	entries, err := t.collectEntries(tbl, -1, false, nil, nil)
	if err != nil {
		return 0, err
	}
	var n int64
	if err := walkGrouped(entries, func(e srcRow) error { n++; return nil }); err != nil {
		return 0, err
	}
	return n, nil
}

func sortPartsBySeq(ps []*mpart) {
	sort.Slice(ps, func(i, j int) bool {
		return ps[i].seq < ps[j].seq
	})
}

// Close flushes buffered rows to parts and removes retired trash.
func (s *mpartStore) Close() error {
	close(s.stopFlush)
	<-s.flushDone
	s.mu.Lock()
	defer s.mu.Unlock()
	for t := range s.mem {
		mp := s.mem[t]
		if mp != nil && len(mp.rows) > 0 {
			if err := s.flushLocked(t); err != nil {
				return err
			}
		}
	}
	for _, tr := range s.trash {
		os.RemoveAll(tr)
	}
	s.trash = nil
	return nil
}

func (s *mpartStore) begin() (storeTx, error) {
	return &mpartTx{s: s, overlay: map[string]*memPart{}}, nil
}

func (s *mpartStore) view(fn func(tx storeTx) error) error {
	t := &mpartTx{s: s, overlay: map[string]*memPart{}}
	defer t.rollback()
	return fn(t)
}

// snapshot captures a point-in-time read-only tx. The current mem parts are
// shallow-copied (rows are immutable, so sharing them is safe) and part
// slices are copied, so later flushes/compacts that reassign them cannot
// affect this tx. The store bumps snapRefs so retired part files stay
// readable until rollback.
func (s *mpartStore) snapshot() (storeTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frozen := map[string]*memPart{}
	for tbl, mp := range s.mem {
		if mp == nil {
			continue
		}
		m := &memPart{rows: make(map[string]*memRow, len(mp.rows))}
		for k, r := range mp.rows {
			m.rows[k] = r
		}
		frozen[tbl] = m
	}
	parts := make(map[string][]*mpart, len(s.parts))
	for tbl, ps := range s.parts {
		parts[tbl] = append([]*mpart(nil), ps...)
	}
	counts := make(map[string]int64, len(s.counts))
	for tbl, c := range s.counts {
		counts[tbl] = c
	}
	s.snapRefs++
	return &mpartTx{s: s, snap: true, snapMem: frozen, snapParts: parts, snapCount: counts}, nil
}

func (s *mpartStore) flush(tbl string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked(tbl)
}

// sortMemRows sorts rows by pk. Most PKs are a single int64 encoded as
// [tInt, 0xff, 8-byte big-endian uint64]; those sort by comparing the tail
// uint64 directly, which is far cheaper than bytes.Compare (especially under
// the CPU contention of a 5-node dev cluster). Anything else falls back to
// bytes.Compare on the full key.
func sortRowsFastOK(rows []*memRow) bool {
	for _, r := range rows {
		// Single int64 PK: [tInt, 0xff, uint64 BE, 0xff] (encodeKey adds a
		// 0xff terminator after every value).
		if len(r.pk) != 11 || r.pk[0] != byte(tInt) || r.pk[1] != 0xff || r.pk[10] != 0xff {
			return false
		}
	}
	return true
}

func sortMemRows(rows []*memRow) {
	if n := len(rows); n > 1 {
		if fast := sortRowsFastOK(rows); fast {
			sort.Slice(rows, func(i, j int) bool {
				return binary.BigEndian.Uint64(rows[i].pk[2:10]) < binary.BigEndian.Uint64(rows[j].pk[2:10])
			})
			return
		}
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].pk, rows[j].pk) < 0 })
}

// memRowsSorted reports whether rows are already in ascending pk order. A
// single-batch UPDATE writes exactly the keys its scan returned (sorted), so
// one linear pass replaces an O(n log n) sort on the flush path.
func memRowsSorted(rows []*memRow) bool {
	for i := 1; i < len(rows); i++ {
		if bytes.Compare(rows[i-1].pk, rows[i].pk) >= 0 {
			return false
		}
	}
	return true
}

// flushLocked writes the current mem part of tbl to a new immutable part and
// resets the mem part. Live counts are unchanged (rows only moved).
func (s *mpartStore) flushLocked(tbl string) error {
	t0 := time.Now()
	mp := s.mem[tbl]
	if mp == nil || len(mp.rows) == 0 {
		return nil
	}
	rows := mp.hint
	if len(rows) != len(mp.rows) || !memRowsSorted(rows) {
		rows = make([]*memRow, 0, len(mp.rows))
		for _, r := range mp.rows {
			rows = append(rows, r)
		}
		if !memRowsSorted(rows) {
			sortMemRows(rows)
		}
	}
	sortDur := time.Since(t0)
	t1 := time.Now()
	s.nextID++
	p, err := s.writePart(tbl, rows, s.nextID)
	writeDur := time.Since(t1)
	if d := time.Since(t0); d > 150*time.Millisecond {
		log.Printf("FLUSH-SLOW: %v (sort %v write %v, %d rows)", d, sortDur, writeDur, len(rows))
	}
	if err != nil {
		return err
	}
	s.parts[tbl] = append(s.parts[tbl], p)
	sortPartsBySeq(s.parts[tbl])
	s.mem[tbl] = &memPart{rows: map[string]*memRow{}}
	return nil
}

// writePart materializes a new immutable part from rows (already sorted by
// pk) into the parts directory, writing column files first and meta.bin last
// as the seal marker.
func (s *mpartStore) writePart(tbl string, rows []*memRow, id int) (*mpart, error) {
	dir := filepath.Join(s.dir, mpartPartsDir, fmt.Sprintf("p%08d", id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ncols := 0
	for _, r := range rows {
		if len(r.cells) > ncols {
			ncols = len(r.cells)
		}
	}
	p := &mpart{
		table:    tbl,
		ncols:    ncols,
		rowcount: len(rows),
		seq:      id,
		dir:      dir,
		bloom:    newMpartBloom(len(rows)),
	}
	// Dense-ness is decided per column across the whole part (the fast
	// aggregate path needs a uniform typed array per column).
	colDense := make([]sqlType, ncols)
	colDenseOK := make([]bool, ncols)
	for c := 0; c < ncols; c++ {
		blobs := make([][]byte, len(rows))
		for i, r := range rows {
			if c < len(r.cells) {
				blobs[i] = r.cells[c]
			}
		}
		if t, ok := denseColumnType(blobs); ok {
			colDense[c], colDenseOK[c] = t, true
			p.colFmts = append(p.colFmts, colFmtDense(t))
		} else {
			p.colFmts = append(p.colFmts, colFmtLegacy)
		}
	}
	// v4: split rows into granule-aligned blocks, each compressed on its own,
	// so a point read or ORDER BY ... LIMIT touches only the block that can
	// hold the key. The sparse index (idx.bin) records per-granule PK windows
	// and block offsets; it is written before meta.bin (the seal), so a crash
	// never leaves an idx.bin without a sealed part.
	var granuleRaw []byte
	if len(rows) > 0 {
		ng := (len(rows) + mpartGranuleRows - 1) / mpartGranuleRows
		p.granules = make([]mpartGranule, ng)
		p.gPksOnce = make([]sync.Once, ng)
		p.gPks = make([][][]byte, ng)
		p.gPksErr = make([]error, ng)
		p.gDelsOnce = make([]sync.Once, ng)
		p.gDels = make([][]bool, ng)
		p.gDelsErr = make([]error, ng)
		p.gColsOnce = make([][]sync.Once, ncols)
		p.gCols = make([][][][]byte, ncols)
		p.gColsErr = make([][]error, ncols)
		if mpartIdxVer >= 3 {
			p.gDenseOnce = make([][]sync.Once, ncols)
			p.gDense = make([][][]int64, ncols)
			p.gDenseNulls = make([][][]uint64, ncols)
			p.gDenseErr = make([][]error, ncols)
		}
		for c := 0; c < ncols; c++ {
			p.gColsOnce[c] = make([]sync.Once, ng)
			p.gCols[c] = make([][][]byte, ng)
			p.gColsErr[c] = make([]error, ng)
			if mpartIdxVer >= 3 {
				p.gDenseOnce[c] = make([]sync.Once, ng)
				p.gDense[c] = make([][]int64, ng)
				p.gDenseNulls[c] = make([][]uint64, ng)
				p.gDenseErr[c] = make([]error, ng)
			}
		}
		for c := 0; c < ncols; c++ {
			for g := range p.granules {
				p.granules[g].colOff = append(p.granules[g].colOff, 0)
				p.granules[g].colLen = append(p.granules[g].colLen, 0)
				p.granules[g].colRaw = append(p.granules[g].colRaw, 0)
				if mpartIdxVer >= 3 {
					p.granules[g].zoneMin = append(p.granules[g].zoneMin, 0)
					p.granules[g].zoneMax = append(p.granules[g].zoneMax, 0)
				}
			}
		}
		// pk.bin: one LZ4 block per granule.
		pkRaw, pkOffs := writeGranuleBlocks(filepath.Join(dir, "pk.bin"), ng, len(rows),
			func(lo, hi int) []byte { return encodeFrames(pkBlobs(rows, lo, hi)) })
		crcs := make([]uint32, 1, ncols+1)
		crcs[0] = crc32.ChecksumIEEE(pkRaw)
		for g := range p.granules {
			p.granules[g].pkOff, p.granules[g].pkLen, p.granules[g].pkRaw = pkOffs[g].off, pkOffs[g].len, pkOffs[g].raw
		}
		for c := 0; c < ncols; c++ {
			blockFn := func(lo, hi int) []byte {
				blobs := make([][]byte, hi-lo)
				for i, r := range rows[lo:hi] {
					if c < len(r.cells) {
						blobs[i] = r.cells[c]
					}
				}
				if colDenseOK[c] {
					return encodeDenseNumeric(blobs, colDense[c])
				}
				return encodeFrames(blobs)
			}
			raw, offs := writeGranuleBlocks(filepath.Join(dir, fmt.Sprintf("col_%d.bin", c)), ng, len(rows), blockFn)
			crcs = append(crcs, crc32.ChecksumIEEE(raw))
			for g := range p.granules {
				p.granules[g].colOff[c], p.granules[g].colLen[c], p.granules[g].colRaw[c] = offs[g].off, offs[g].len, offs[g].raw
			}
		}
		// del.bin: one LZ4 block per granule (bitmap of tombstone rows).
		delFn := func(lo, hi int) []byte {
			bits := make([]byte, (hi-lo+7)/8)
			for i, r := range rows[lo:hi] {
				if r.del {
					bits[i/8] |= 1 << (i & 7)
				}
			}
			return bits
		}
		_, delOffs := writeGranuleBlocks(filepath.Join(dir, "del.bin"), ng, len(rows), delFn)
		for g := range p.granules {
			p.granules[g].delOff, p.granules[g].delLen, p.granules[g].delRaw = delOffs[g].off, delOffs[g].len, delOffs[g].raw
		}
		// Record each granule's PK window (last PK of the block) and the
		// per-dense-column value zones (idx ver 3) so filtered scans can skip
		// granules that cannot match a numeric WHERE predicate.
		for g := range p.granules {
			lo := g * mpartGranuleRows
			hi := lo + mpartGranuleRows
			if hi > len(rows) {
				hi = len(rows)
			}
			p.granules[g].pkMax = append([]byte(nil), rows[hi-1].pk...)
			if mpartIdxVer >= 3 {
				for c := 0; c < ncols; c++ {
					if !colDenseOK[c] {
						continue
					}
					minV, maxV := granuleNumericZone(rows[lo:hi], c, colDense[c])
					p.granules[g].zoneMin[c] = minV
					p.granules[g].zoneMax[c] = maxV
				}
			}
		}
		granuleRaw = encodeGranuleIndex(p.granules, ncols, p.colFmts)
		if err := os.WriteFile(filepath.Join(dir, "idx.bin"), granuleRaw, 0o644); err != nil {
			return nil, err
		}
		p.crcs = crcs
	} else {
		// Empty part: keep the v2/v3 single-block layout (no granules).
		pkBlobs := make([][]byte, 0)
		pkRaw := encodeFrames(pkBlobs)
		crcs := make([]uint32, 1, ncols+1)
		crcs[0] = crc32.ChecksumIEEE(pkRaw)
		if err := os.WriteFile(filepath.Join(dir, "pk.bin"), mustLZ4(pkRaw), 0o644); err != nil {
			return nil, err
		}
		for c := 0; c < ncols; c++ {
			blobs := make([][]byte, 0)
			var raw []byte
			if colDenseOK[c] {
				raw = encodeDenseNumeric(blobs, colDense[c])
			} else {
				raw = encodeFrames(blobs)
			}
			crcs = append(crcs, crc32.ChecksumIEEE(raw))
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("col_%d.bin", c)), mustLZ4(raw), 0o644); err != nil {
				return nil, err
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "del.bin"), mustLZ4(nil), 0o644); err != nil {
			return nil, err
		}
		p.crcs = crcs
	}
	if len(rows) > 0 {
		p.pkMin = append([]byte(nil), rows[0].pk...)
		p.pkMax = append([]byte(nil), rows[len(rows)-1].pk...)
		for _, r := range rows {
			p.bloom.add(r.pk)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.bin"), p.encodeMeta(), 0o644); err != nil {
		return nil, err
	}
	return p, nil
}

// pkBlobs returns the raw PK bytes of rows[lo:hi).
func pkBlobs(rows []*memRow, lo, hi int) [][]byte {
	out := make([][]byte, hi-lo)
	for i, r := range rows[lo:hi] {
		out[i] = r.pk
	}
	return out
}

// granuleNumericZone returns the (min, max) of a numeric column over the given
// rows, as the raw encoded values (zigzag-decoded ints; float64 bit patterns
// for tFloat). NULL cells are skipped; a granule with only NULLs yields
// (0, 0), which never falsely matches a non-NULL predicate.
func granuleNumericZone(rows []*memRow, colIdx int, typ sqlType) (min, max int64) {
	min, max = 0, 0
	first := true
	for _, r := range rows {
		if colIdx >= len(r.cells) {
			continue
		}
		val, f, null, ok := decodeNumericCell(r.cells[colIdx], typ)
		if !ok || null {
			continue
		}
		if typ == tFloat {
			val = int64(math.Float64bits(f))
		}
		if first {
			min, max, first = val, val, false
			continue
		}
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
	}
	return min, max
}

// blockOff is a single granule block's byte range within its file. raw is the
// uncompressed size (raw LZ4 blocks, idx ver 2); 0 means a framed block
// (idx ver 1), decompressed via lz4Expand.
type blockOff struct{ off, len, raw int64 }

// writeGranuleBlocks writes ng independent LZ4 blocks (one per granule) to
// path and returns the concatenated raw data plus each block's offset/length.
// total is the number of rows, so the final granule's hi is clamped to it.
func writeGranuleBlocks(path string, ng, total int, blockFn func(lo, hi int) []byte) ([]byte, []blockOff) {
	var raw []byte
	offs := make([]blockOff, ng)
	for g := 0; g < ng; g++ {
		lo := g * mpartGranuleRows
		hi := lo + mpartGranuleRows
		if hi > total {
			hi = total
		}
		plain := blockFn(lo, hi)
		enc := mustLZ4Block(plain)
		offs[g] = blockOff{int64(len(raw)), int64(len(enc)), int64(len(plain))}
		raw = append(raw, enc...)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		// writeGranuleBlocks is called with a valid path by writePart only.
		panic(err)
	}
	return raw, offs
}

// mustLZ4Block compresses plain as a raw LZ4 block (no framing, no checksum):
// decompression is a single UncompressBlock into a known-size buffer, ~6x
// faster than the framed stream. An incompressible block is stored as-is and
// the reader detects n==0 (compressed length) to copy it without decompressing.
func mustLZ4Block(plain []byte) []byte {
	dst := make([]byte, lz4.CompressBlockBound(len(plain)))
	n, err := lz4.CompressBlock(plain, dst, nil)
	if err != nil || n == 0 {
		return plain
	}
	return dst[:n]
}

// encodeGranuleIndex serializes the sparse index (idx.bin): the number of
// granules, each granule's PK window and per-file block offsets. Uncompressed
// and tiny (one entry per mpartGranuleRows rows). Index ver 3 additionally
// stores per-dense-column zone maps (min/max over each granule's rows) so
// filtered scans can skip granules that cannot match a numeric predicate.
func encodeGranuleIndex(gs []mpartGranule, ncols int, colFmts []byte) []byte {
	b := makeBuilder()
	b.Str(mpartIdxMagic)
	b.Var(mpartIdxVer)
	b.Var(int64(ncols))
	b.Var(int64(len(gs)))
	for i := range gs {
		b.Str(string(gs[i].pkMax))
		b.Var(gs[i].pkOff)
		b.Var(gs[i].pkLen)
		b.Var(gs[i].pkRaw)
		b.Var(gs[i].delOff)
		b.Var(gs[i].delLen)
		b.Var(gs[i].delRaw)
		for c := 0; c < ncols; c++ {
			b.Var(gs[i].colOff[c])
			b.Var(gs[i].colLen[c])
			b.Var(gs[i].colRaw[c])
		}
		if mpartIdxVer >= 3 {
			for c := 0; c < ncols; c++ {
				if _, dense := colFmtType(colFmts[c]); !dense {
					continue
				}
				b.Var(gs[i].zoneMin[c])
				b.Var(gs[i].zoneMax[c])
			}
		}
	}
	return b.Bytes()
}

func decodeGranuleIndex(raw []byte, ncols int, colFmts []byte) ([]mpartGranule, error) {
	r := makeReader(raw)
	if r.Str() != mpartIdxMagic {
		return nil, errors.New("bad mpart idx magic")
	}
	ver := r.Var()
	if ver != 1 && ver != 2 && ver != 3 {
		return nil, errors.New("bad mpart idx version")
	}
	if r.Var() != int64(ncols) {
		return nil, errors.New("mpart idx ncols mismatch")
	}
	ng := int(r.Var())
	if ng < 0 || r.err != nil {
		return nil, r.err
	}
	gs := make([]mpartGranule, ng)
	for i := range gs {
		gs[i].pkMax = r.Bytes()
		gs[i].pkOff = r.Var()
		gs[i].pkLen = r.Var()
		if ver >= 2 {
			gs[i].pkRaw = r.Var()
		}
		gs[i].delOff = r.Var()
		gs[i].delLen = r.Var()
		if ver >= 2 {
			gs[i].delRaw = r.Var()
		}
		for c := 0; c < ncols; c++ {
			gs[i].colOff = append(gs[i].colOff, r.Var())
			gs[i].colLen = append(gs[i].colLen, r.Var())
			if ver >= 2 {
				gs[i].colRaw = append(gs[i].colRaw, r.Var())
			} else {
				gs[i].colRaw = append(gs[i].colRaw, 0)
			}
		}
		if ver >= 3 {
			gs[i].zoneMin = make([]int64, ncols)
			gs[i].zoneMax = make([]int64, ncols)
			for c := 0; c < ncols; c++ {
				if _, dense := colFmtType(colFmts[c]); !dense {
					continue
				}
				gs[i].zoneMin[c] = r.Var()
				gs[i].zoneMax[c] = r.Var()
			}
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	return gs, nil
}

// granuleFor returns the index of the granule that can contain pk, or -1.
func (p *mpart) granuleFor(pk []byte) int {
	if len(p.granules) == 0 {
		return -1
	}
	i := sort.Search(len(p.granules), func(i int) bool { return bytes.Compare(p.granules[i].pkMax, pk) >= 0 })
	if i >= len(p.granules) {
		return -1
	}
	return i
}

// granuleZoneHits reports whether granule g could contain a row whose column
// colIdx satisfies a numeric equality predicate, using the per-granule zone
// maps (idx ver 3). A nil zone (legacy column / no zones) always reports true
// so the caller falls back to decoding the granule.
func (p *mpart) granuleZoneHits(g, colIdx int, pred *sqlx.ColumnFilter, ctyp sqlType) bool {
	if g < 0 || g >= len(p.granules) || colIdx >= len(p.granules[g].zoneMin) {
		return true
	}
	lit := fromSQLValue(pred.Lit)
	if lit.null {
		return false
	}
	var lo, hi int64
	switch ctyp {
	case tFloat:
		lo, hi = p.granules[g].zoneMin[colIdx], p.granules[g].zoneMax[colIdx]
		lf := lit.f
		return lf >= math.Float64frombits(uint64(lo)) && lf <= math.Float64frombits(uint64(hi))
	case tInt, tTimestamp:
		lo, hi = p.granules[g].zoneMin[colIdx], p.granules[g].zoneMax[colIdx]
		return lit.i >= lo && lit.i <= hi
	default:
		return true
	}
}

// readGranule reads the granule g's LZ4 block from path and expands it.
func (p *mpart) readGranule(path string, off, ln, raw int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, ln)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return expandBlock(buf, raw)
}

// expandBlock decompresses one granule block. raw is the uncompressed size
// (idx ver 2, raw LZ4 block); raw==0 means an idx ver 1 framed block decoded
// via lz4Expand. A block whose compressed length equals its uncompressed size
// was stored incompressible and is returned as-is.
func expandBlock(blk []byte, raw int64) ([]byte, error) {
	if raw <= 0 {
		return lz4Expand(blk)
	}
	if int64(len(blk)) == raw {
		return blk, nil
	}
	dst := make([]byte, raw)
	if _, err := lz4.UncompressBlock(blk, dst); err != nil {
		return nil, err
	}
	return dst, nil
}

// encodeMeta serializes the part metadata (uncompressed; it is tiny).
func (p *mpart) encodeMeta() []byte {
	b := makeBuilder()
	b.Str(mpartMagic)
	b.Var(mpartVersion)
	b.Str(p.table)
	b.Var(int64(p.ncols))
	b.Var(int64(p.rowcount))
	b.Str(string(p.pkMin))
	b.Str(string(p.pkMax))
	b.Var(int64(len(p.bloom.bits)))
	for _, w := range p.bloom.bits {
		b.buf = binary.LittleEndian.AppendUint64(b.buf, w)
	}
	b.Var(int64(len(p.crcs)))
	for _, c := range p.crcs {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, c)
	}
	if mpartVersion >= 3 {
		b.Var(int64(len(p.colFmts)))
		b.buf = append(b.buf, p.colFmts...)
	}
	return b.Bytes()
}

func readMpartMeta(dir string) (*mpart, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "meta.bin"))
	if err != nil {
		return nil, err
	}
	r := makeReader(raw)
	if r.Str() != mpartMagic {
		return nil, errors.New("bad mpart magic")
	}
	ver := r.Var()
	if ver != 2 && ver != 3 && ver != 4 {
		return nil, errors.New("unsupported mpart version")
	}
	p := &mpart{table: r.Str(), dir: dir}
	p.ncols = int(r.Var())
	p.rowcount = int(r.Var())
	p.pkMin = r.Bytes()
	p.pkMax = r.Bytes()
	nw := int(r.Var())
	p.bloom = &mpartBloom{bits: make([]uint64, nw)}
	raw = r.Take(nw * 8)
	for i := 0; i < nw; i++ {
		p.bloom.bits[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	nc := int(r.Var())
	p.crcs = make([]uint32, nc)
	crcRaw := r.Take(nc * 4)
	for i := 0; i < nc; i++ {
		p.crcs[i] = binary.LittleEndian.Uint32(crcRaw[i*4:])
	}
	if ver >= 3 {
		nf := int(r.Var())
		p.colFmts = r.Take(nf)
		if len(p.colFmts) < p.ncols {
			return nil, errors.New("short colFmts")
		}
	}
	if ver >= 4 {
		// v4 parts carry the sparse granule index in idx.bin.
		idxRaw, err := os.ReadFile(filepath.Join(dir, "idx.bin"))
		if err != nil {
			return nil, err
		}
		gs, err := decodeGranuleIndex(idxRaw, p.ncols, p.colFmts)
		if err != nil {
			return nil, err
		}
		p.granules = gs
		p.gPksOnce = make([]sync.Once, len(gs))
		p.gPks = make([][][]byte, len(gs))
		p.gPksErr = make([]error, len(gs))
		p.gDelsOnce = make([]sync.Once, len(gs))
		p.gDels = make([][]bool, len(gs))
		p.gDelsErr = make([]error, len(gs))
		p.gColsOnce = make([][]sync.Once, p.ncols)
		p.gCols = make([][][][]byte, p.ncols)
		p.gColsErr = make([][]error, p.ncols)
		p.gDenseOnce = make([][]sync.Once, p.ncols)
		p.gDense = make([][][]int64, p.ncols)
		p.gDenseNulls = make([][][]uint64, p.ncols)
		p.gDenseErr = make([][]error, p.ncols)
		for c := 0; c < p.ncols; c++ {
			p.gColsOnce[c] = make([]sync.Once, len(gs))
			p.gCols[c] = make([][][]byte, len(gs))
			p.gColsErr[c] = make([]error, len(gs))
			p.gDenseOnce[c] = make([]sync.Once, len(gs))
			p.gDense[c] = make([][]int64, len(gs))
			p.gDenseNulls[c] = make([][]uint64, len(gs))
			p.gDenseErr[c] = make([]error, len(gs))
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	return p, nil
}

// granuleRowRange returns the row index range of granule g.
func (p *mpart) granuleRowRange(g int) (int, int) {
	lo := g * mpartGranuleRows
	hi := lo + mpartGranuleRows
	if hi > p.rowcount {
		hi = p.rowcount
	}
	return lo, hi
}

// loadGranulePks decodes just granule g's PK block (v4 parts).
func (p *mpart) loadGranulePks(g int) ([][]byte, error) {
	if g < 0 || g >= len(p.granules) {
		return nil, nil
	}
	p.gPksOnce[g].Do(func() {
		dec, err := p.readGranule(filepath.Join(p.dir, "pk.bin"), p.granules[g].pkOff, p.granules[g].pkLen, p.granules[g].pkRaw)
		if err != nil {
			p.gPksErr[g] = err
			return
		}
		p.gPks[g] = decodeFrames(dec)
	})
	return p.gPks[g], p.gPksErr[g]
}

// loadGranuleDels decodes just granule g's tombstone bits (v4 parts).
func (p *mpart) loadGranuleDels(g int) ([]bool, error) {
	if g < 0 || g >= len(p.granules) {
		return nil, nil
	}
	p.gDelsOnce[g].Do(func() {
		lo, hi := p.granuleRowRange(g)
		dec, err := p.readGranule(filepath.Join(p.dir, "del.bin"), p.granules[g].delOff, p.granules[g].delLen, p.granules[g].delRaw)
		if err != nil {
			p.gDelsErr[g] = err
			return
		}
		out := make([]bool, hi-lo)
		for i := range out {
			if dec[i/8]&(1<<(i&7)) != 0 {
				out[i] = true
			}
		}
		p.gDels[g] = out
	})
	return p.gDels[g], p.gDelsErr[g]
}

// loadGranuleDense decodes just granule g's numeric cells of one dense column
// into packed (vals, nulls) form (v4 parts). The result is cached; it is used
// by zone-map-filtered scans that skip granules whose value bounds cannot
// match the predicate.
func (p *mpart) loadGranuleDense(g, colIdx int) ([]int64, []uint64, error) {
	if g < 0 || g >= len(p.granules) || colIdx >= p.ncols {
		return nil, nil, nil
	}
	if colIdx >= len(p.gDenseOnce) {
		return nil, nil, nil
	}
	p.gDenseOnce[colIdx][g].Do(func() {
		raw, err := p.readGranule(p.colPath(colIdx), p.granules[g].colOff[colIdx], p.granules[g].colLen[colIdx], p.granules[g].colRaw[colIdx])
		if err != nil {
			p.gDenseErr[colIdx][g] = err
			return
		}
		vals, nulls, err := decodeDenseNumeric(raw)
		if err != nil {
			p.gDenseErr[colIdx][g] = err
			return
		}
		p.gDense[colIdx][g] = vals
		p.gDenseNulls[colIdx][g] = nulls
	})
	return p.gDense[colIdx][g], p.gDenseNulls[colIdx][g], p.gDenseErr[colIdx][g]
}

// loadGranuleCol decodes just granule g's cells of one column (v4 parts). The
// returned slice is indexed from 0 within the granule.
func (p *mpart) loadGranuleCol(g, colIdx int) ([][]byte, error) {
	if g < 0 || g >= len(p.granules) || colIdx >= p.ncols {
		return nil, nil
	}
	p.gColsOnce[colIdx][g].Do(func() {
		raw, err := p.readGranule(p.colPath(colIdx), p.granules[g].colOff[colIdx], p.granules[g].colLen[colIdx], p.granules[g].colRaw[colIdx])
		if err != nil {
			p.gColsErr[colIdx][g] = err
			return
		}
		if colIdx < len(p.colFmts) {
			if t, dense := colFmtType(p.colFmts[colIdx]); dense {
				vals, nulls, err := decodeDenseNumeric(raw)
				if err != nil {
					p.gColsErr[colIdx][g] = err
					return
				}
				p.gCols[colIdx][g] = denseToCells(vals, nulls, t)
				return
			}
		}
		p.gCols[colIdx][g] = decodeFrames(raw)
	})
	return p.gCols[colIdx][g], p.gColsErr[colIdx][g]
}

// loadPks decompresses and decodes the part's PK list (sorted ascending),
// memoizing the result because parts are immutable. For granule-aligned parts
// all granule blocks are concatenated in order.
func (p *mpart) loadPks() ([][]byte, error) {
	p.pksOnce.Do(func() {
		if len(p.granules) > 0 {
			raw, err := os.ReadFile(filepath.Join(p.dir, "pk.bin"))
			if err != nil {
				p.pksErr = err
				return
			}
			for g := range p.granules {
				blk := raw[p.granules[g].pkOff : p.granules[g].pkOff+p.granules[g].pkLen]
				dec, err := expandBlock(blk, p.granules[g].pkRaw)
				if err != nil {
					p.pksErr = err
					return
				}
				p.pks = append(p.pks, decodeFrames(dec)...)
			}
			return
		}
		raw, err := os.ReadFile(filepath.Join(p.dir, "pk.bin"))
		if err != nil {
			p.pksErr = err
			return
		}
		dec, err := lz4Expand(raw)
		if err != nil {
			p.pksErr = err
			return
		}
		p.pks = decodeFrames(dec)
	})
	return p.pks, p.pksErr
}

// loadDels decompresses the part's tombstone bitmap (one bit per row),
// memoizing the result because parts are immutable.
func (p *mpart) loadDels() ([]bool, error) {
	p.delsOnce.Do(func() {
		p.dels = make([]bool, p.rowcount)
		if len(p.granules) > 0 {
			raw, err := os.ReadFile(filepath.Join(p.dir, "del.bin"))
			if err != nil {
				p.delsErr = err
				return
			}
			for g := range p.granules {
				lo, hi := p.granuleRowRange(g)
				blk := raw[p.granules[g].delOff : p.granules[g].delOff+p.granules[g].delLen]
				dec, err := expandBlock(blk, p.granules[g].delRaw)
				if err != nil {
					p.delsErr = err
					return
				}
				for i := lo; i < hi; i++ {
					if dec[(i-lo)/8]&(1<<((i-lo)&7)) != 0 {
						p.dels[i] = true
					}
				}
			}
			return
		}
		raw, err := os.ReadFile(filepath.Join(p.dir, "del.bin"))
		if err != nil {
			if os.IsNotExist(err) {
				return // legacy part without del.bin: no tombstones
			}
			p.delsErr = err
			return
		}
		dec, err := lz4Expand(raw)
		if err != nil {
			p.delsErr = err
			return
		}
		for i := range p.dels {
			if dec[i/8]&(1<<(i&7)) != 0 {
				p.dels[i] = true
			}
		}
	})
	return p.dels, p.delsErr
}

// loadCol returns the part's sorted PKs and one column's cells (parallel).
func (p *mpart) loadCol(colIdx int) ([][]byte, [][]byte, error) {
	pks, err := p.loadPks()
	if err != nil {
		return nil, nil, err
	}
	if colIdx < 0 || colIdx >= p.ncols {
		// The part has no column data (e.g. a tombstone-only part produced by a
		// range DELETE). Keep cells length-synced with pks so column walks see
		// one (nil) cell per row; deleted rows are filtered by their tombstone
		// bit before the nil cell is ever read.
		return pks, make([][]byte, len(pks)), nil
	}
	if p.colsOnce == nil {
		p.colsOnce = make([]sync.Once, p.ncols)
		p.cols = make([][][]byte, p.ncols)
		p.colsErr = make([]error, p.ncols)
	}
	p.colsOnce[colIdx].Do(func() {
		if len(p.granules) > 0 {
			// v4 part: decode each granule block in order.
			raw, err := os.ReadFile(p.colPath(colIdx))
			if err != nil {
				p.colsErr[colIdx] = err
				return
			}
			if colIdx < len(p.colFmts) {
				if t, dense := colFmtType(p.colFmts[colIdx]); dense {
					var vals []int64
					var nulls []uint64
					for g := range p.granules {
						blk := raw[p.granules[g].colOff[colIdx] : p.granules[g].colOff[colIdx]+p.granules[g].colLen[colIdx]]
						dec, err := expandBlock(blk, p.granules[g].colRaw[colIdx])
						if err != nil {
							p.colsErr[colIdx] = err
							return
						}
						gv, gn, err := decodeDenseNumeric(dec)
						if err != nil {
							p.colsErr[colIdx] = err
							return
						}
						vals = append(vals, gv...)
						nulls = append(nulls, gn...)
					}
					p.cols[colIdx] = denseToCells(vals, nulls, t)
					return
				}
			}
			var out [][]byte
			for g := range p.granules {
				blk := raw[p.granules[g].colOff[colIdx] : p.granules[g].colOff[colIdx]+p.granules[g].colLen[colIdx]]
				dec, err := expandBlock(blk, p.granules[g].colRaw[colIdx])
				if err != nil {
					p.colsErr[colIdx] = err
					return
				}
				out = append(out, decodeFrames(dec)...)
			}
			p.cols[colIdx] = out
			return
		}
		if colIdx < len(p.colFmts) {
			if t, dense := colFmtType(p.colFmts[colIdx]); dense {
				// Dense numeric column: rebuild per-cell blobs from the dense
				// array for consumers that still want cells (full-row scans,
				// point reads, ORDER BY). Aggregates use loadColDense instead.
				vals, nulls, _, err := p.loadColDense(colIdx)
				if err != nil {
					p.colsErr[colIdx] = err
					return
				}
				p.cols[colIdx] = denseToCells(vals, nulls, t)
				return
			}
		}
		raw, err := os.ReadFile(p.colPath(colIdx))
		if err != nil {
			p.colsErr[colIdx] = err
			return
		}
		dec, err := lz4Expand(raw)
		if err != nil {
			p.colsErr[colIdx] = err
			return
		}
		p.cols[colIdx] = decodeFrames(dec)
	})
	return pks, p.cols[colIdx], p.colsErr[colIdx]
}

// loadColDense returns one column's values as a dense fixed-width int64 array
// and null bitmap, when the column is stored in the dense layout. ok=false
// means the column uses the legacy frames layout. PKs are NOT loaded here;
// callers that need them load them via loadPks. Memoized per column because
// parts are immutable.
func (p *mpart) loadColDense(colIdx int) (vals []int64, nulls []uint64, ok bool, err error) {
	if colIdx < 0 || colIdx >= p.ncols || colIdx >= len(p.colFmts) {
		return nil, nil, false, nil
	}
	if _, dense := colFmtType(p.colFmts[colIdx]); !dense {
		return nil, nil, false, nil
	}
	if p.denseOnce == nil {
		// Publish the backing arrays before denseOnce so a concurrent reader
		// that observes denseOnce != nil can never see nil denseVals/denseNulls
		// (which would panic inside the Once body). denseOnce is written last
		// and acts as the readiness marker; unsynchronized readers still see a
		// torn state only until denseOnce lands, and the Once body is guarded
		// by denseOnce[colIdx] itself.
		once := make([]sync.Once, p.ncols)
		vals := make([][]int64, p.ncols)
		nulls := make([][]uint64, p.ncols)
		errs := make([]error, p.ncols)
		p.denseVals = vals
		p.denseNulls = nulls
		p.denseErr = errs
		p.denseOnce = once
	}
	p.denseOnce[colIdx].Do(func() {
		if len(p.granules) > 0 {
			// v4 part: decode each granule's dense block and concatenate. The
			// blocks are independently compressed, so they decode in parallel;
			// this is the bulk of a cold SUM/GROUP over a large merged part.
			vals, nulls, err := p.decodeDenseColGranules(colIdx)
			if err != nil {
				p.denseErr[colIdx] = err
				return
			}
			p.denseVals[colIdx] = vals
			p.denseNulls[colIdx] = nulls
			return
		}
		raw, err := os.ReadFile(p.colPath(colIdx))
		if err != nil {
			p.denseErr[colIdx] = err
			return
		}
		dec, err := lz4Expand(raw)
		if err != nil {
			p.denseErr[colIdx] = err
			return
		}
		vals, nulls, err := decodeDenseNumeric(dec)
		if err != nil {
			p.denseErr[colIdx] = err
			return
		}
		p.denseVals[colIdx] = vals
		p.denseNulls[colIdx] = nulls
	})
	return p.denseVals[colIdx], p.denseNulls[colIdx], true, p.denseErr[colIdx]
}

// preloadDense eagerly decodes every dense numeric column into the part's
// memoized dense cache, so the first SUM/GROUP query after an idle flush/merge
// finds warm values instead of paying the cold LZ4 pass inline. Runs on the
// idle-flusher goroutine; the cache is populated by the same loadColDense
// paths the query would use, so this is pure prefetch.
func (p *mpart) preloadDense() {
	if p == nil {
		return
	}
	p.preloadOnce.Do(func() {
		var wg sync.WaitGroup
		for c := 0; c < p.ncols; c++ {
			if c >= len(p.colFmts) {
				continue
			}
			if _, dense := colFmtType(p.colFmts[c]); !dense {
				continue
			}
			wg.Add(1)
			go func(col int) {
				defer wg.Done()
				_, _, _, _ = p.loadColDense(col)
			}(c)
		}
		wg.Wait()
	})
}

// loadRows returns the sorted PKs and every column's cells (parallel, full
// width) for full-row scans. Column data comes from the memoized per-column
// cache (parts are immutable).
func (p *mpart) loadRows() ([][]byte, [][][]byte, error) {
	pks, err := p.loadPks()
	if err != nil {
		return nil, nil, err
	}
	full := make([][][]byte, len(pks))
	for c := 0; c < p.ncols; c++ {
		_, cells, err := p.loadCol(c)
		if err != nil {
			return nil, nil, err
		}
		for i := range full {
			full[i] = append(full[i], cells[i])
		}
	}
	return pks, full, nil
}

// retirePart moves a part's files out of the live tree. If any snapshot tx is
// still reading, deletion is deferred until the last snapshot releases.
func (s *mpartStore) retirePart(p *mpart) {
	if s.snapRefs > 0 {
		tr := filepath.Join(s.dir, "trash", filepath.Base(p.dir))
		if err := os.Rename(p.dir, tr); err == nil {
			s.trash = append(s.trash, tr)
			return
		}
	}
	os.RemoveAll(p.dir)
}

// mpartTx implements storeTx over an mpartStore.
type mpartTx struct {
	s *mpartStore

	// overlay holds this transaction's writes (tables -> rows). It shadows
	// committed state with priority: overlay > mem > parts.
	overlay map[string]*memPart
	cleared map[string]bool
	deltas  map[string]int64

	snap      bool
	snapMem   map[string]*memPart
	snapParts map[string][]*mpart
	snapCount map[string]int64
}

// schema definitions live in the default (TABLE) store, so the engine store's
// schema methods are no-ops: they are never routed here.

func (t *mpartTx) schemaPut(name string, def []byte) error { return nil }
func (t *mpartTx) schemaGet(name string) ([]byte, error)   { return nil, nil }
func (t *mpartTx) schemaDelete(name string) error          { return nil }
func (t *mpartTx) schemaNames() ([]string, error)          { return nil, nil }

func (t *mpartTx) delta(tbl string, d int64) {
	if t.deltas == nil {
		t.deltas = map[string]int64{}
	}
	t.deltas[tbl] += d
}

func (t *mpartTx) overlayPart(tbl string) *memPart {
	mp := t.overlay[tbl]
	if mp == nil {
		mp = &memPart{rows: map[string]*memRow{}}
		t.overlay[tbl] = mp
	}
	return mp
}

// committedViewLocked exposes the committed (non-overlay) state for the read
// path: mem+parts normally, snapshots' frozen copies when t.snap is set.
// Caller must hold s.mu (or t.snap, whose copies are immutable).
func (t *mpartTx) committedViewLocked(tbl string) (parts []*mpart, mem *memPart) {
	if t.snap {
		return t.snapParts[tbl], t.snapMem[tbl]
	}
	return t.s.parts[tbl], t.s.mem[tbl]
}

// committedView is committedViewLocked plus the store lock.
func (t *mpartTx) committedView(tbl string) (parts []*mpart, mem *memPart) {
	if t.snap {
		return t.snapParts[tbl], t.snapMem[tbl]
	}
	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.parts[tbl], s.mem[tbl]
}

func (t *mpartTx) rowPut(table string, key []byte, val []byte) error {
	n, cells, err := splitRow(val)
	if err != nil {
		return err
	}
	_ = n
	return t.rowPutCells(table, key, cells)
}

// rowPutCells buffers a row from already-decoded column cells, skipping the
// encodeRow->splitRow round trip that rowPut performs for callers that only
// have the encoded row blob.
func (t *mpartTx) rowPutCells(table string, key []byte, cells [][]byte) error {
	// A row that becomes live raises the table's count; overwriting a live row
	// does not. The existence read goes through the overlay so repeated writes
	// of one pk in a single tx are exact.
	return t.rowPutCellsCheckCount(table, key, cells, true)
}

// rowPutCellsNoCount is rowPutCells without the existence probe used to keep
// the live-row count in sync. Callers that know every key already exists (the
// columnar UPDATE batch: execUpdateColumnar only re-writes rows it scanned)
// skip the bloom+granule lookup per row.
func (t *mpartTx) rowPutCellsNoCount(table string, key []byte, cells [][]byte) error {
	return t.rowPutCellsCheckCount(table, key, cells, false)
}

func (t *mpartTx) rowPutCellsCheckCount(table string, key []byte, cells [][]byte, check bool) error {
	if check {
		exists, err := t.rowExists(table, key)
		if err != nil {
			return err
		}
		if !exists {
			t.delta(table, 1)
		}
	}
	mp := t.overlayPart(table)
	r := &memRow{
		pk:    append([]byte(nil), key...),
		cells: cells,
	}
	if _, dup := mp.rows[string(key)]; !dup {
		mp.hint = append(mp.hint, r)
	} else {
		mp.hint = nil
	}
	mp.rows[string(key)] = r
	return nil
}

func (t *mpartTx) rowGet(table string, key []byte) ([]byte, error) {
	cells, del, found, err := t.lookup(table, key)
	if err != nil || !found || del {
		return nil, err
	}
	return joinRow(cells), nil
}

func (t *mpartTx) rowDelete(table string, key []byte) error {
	exists, err := t.rowExists(table, key)
	if err != nil {
		return err
	}
	if exists {
		t.delta(table, -1)
	}
	t.overlayPart(table).rows[string(key)] = &memRow{pk: append([]byte(nil), key...), del: true}
	return nil
}

// rowExists reports whether key is live in the current view (overlay, mem or
// any part), ignoring this tx's own tombstones.
func (t *mpartTx) rowExists(table string, key []byte) (bool, error) {
	if ov := t.overlay[table]; ov != nil {
		if r, ok := ov.rows[string(key)]; ok {
			return !r.del, nil
		}
	}
	if t.cleared[table] {
		return false, nil
	}
	parts, mem := t.committedView(table)
	if mem != nil {
		if r, ok := mem.rows[string(key)]; ok {
			return !r.del, nil
		}
	}
	// Newest part first, decoding only pk + tombstone bits of the containing
	// granule: existence checks (UPSERT dedup) must not decompress every
	// column's cells of a granule just to answer found&&!del.
	for i := len(parts) - 1; i >= 0; i-- {
		p := parts[i]
		if bytes.Compare(key, p.pkMin) < 0 || bytes.Compare(key, p.pkMax) > 0 {
			continue
		}
		if !p.bloom.mayContain(key) {
			continue
		}
		if g := p.granuleFor(key); g >= 0 {
			pks, err := p.loadGranulePks(g)
			if err != nil {
				return false, err
			}
			gi := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
			if gi < len(pks) && bytes.Equal(pks[gi], key) {
				dels, err := p.loadGranuleDels(g)
				if err != nil {
					return false, err
				}
				return !dels[gi], nil
			}
			continue
		}
		if len(p.granules) > 0 {
			continue
		}
		pks, _, lerr := p.loadRows()
		if lerr != nil {
			return false, lerr
		}
		dels, derr := p.loadDels()
		if derr != nil {
			return false, derr
		}
		i := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
		if i < len(pks) && bytes.Equal(pks[i], key) {
			return !dels[i], nil
		}
	}
	return false, nil
}

// lookup resolves key to its newest live-or-deleted row: overlay first, then
// committed mem, then parts (via bloom + binary search).
func (t *mpartTx) lookup(table string, key []byte) (cells [][]byte, del, found bool, err error) {
	if ov := t.overlay[table]; ov != nil {
		if r, ok := ov.rows[string(key)]; ok {
			if r.del {
				return nil, true, true, nil
			}
			if !hasEmptyCell(r.cells) {
				return r.cells, false, true, nil
			}
			cells = append([][]byte(nil), r.cells...)
			found = true
		}
	}
	if t.cleared[table] {
		if found {
			return cells, false, true, nil
		}
		return nil, false, false, nil
	}
	parts, mem := t.committedView(table)
	if mem != nil {
		if r, ok := mem.rows[string(key)]; ok {
			if r.del {
				if found {
					return cells, false, true, nil
				}
				return nil, true, true, nil
			}
			if !found {
				if !hasEmptyCell(r.cells) {
					return r.cells, false, true, nil
				}
				cells = append([][]byte(nil), r.cells...)
				found = true
			} else {
				fillEmptyCells(cells, r.cells)
				if !hasEmptyCell(cells) {
					return cells, false, true, nil
				}
			}
		}
	}
	// Newest part first: a pk may live in several parts (an update rewrites a
	// pk that already lives in an older part), and the newest one wins. A
	// partial newest version inherits its empty columns from the next part.
	for i := len(parts) - 1; i >= 0; i-- {
		p := parts[i]
		if !p.bloom.mayContain(key) {
			continue
		}
		var pr [][]byte
		var pd bool
		if g := p.granuleFor(key); g >= 0 {
			// v4 part: decode only the granule that can hold the key.
			pks, gerr := p.loadGranulePks(g)
			if gerr != nil {
				return nil, false, false, gerr
			}
			gi := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
			if gi < len(pks) && bytes.Equal(pks[gi], key) {
				dels, derr := p.loadGranuleDels(g)
				if derr != nil {
					return nil, false, false, derr
				}
				full, ferr := p.loadGranuleFull(g)
				if ferr != nil {
					return nil, false, false, ferr
				}
				pr, pd = full[gi], dels[gi]
			} else {
				continue
			}
		} else if len(p.granules) > 0 {
			// v4 part but the key lies outside the part's PK span (a bloom
			// false positive): skip without decompressing.
			continue
		} else {
			pks, full, lerr := p.loadRows()
			if lerr != nil {
				return nil, false, false, lerr
			}
			dels, derr := p.loadDels()
			if derr != nil {
				return nil, false, false, derr
			}
			gi := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
			if gi < len(pks) && bytes.Equal(pks[gi], key) {
				pr, pd = full[gi], dels[gi]
			} else {
				continue
			}
		}
		if pd {
			// A tombstone is the newest live-or-deleted version only when no
			// newer live version exists.
			if !found {
				return nil, true, true, nil
			}
			continue
		}
		if !found {
			if !hasEmptyCell(pr) {
				return pr, false, true, nil
			}
			cells = append([][]byte(nil), pr...)
			found = true
		} else {
			fillEmptyCells(cells, pr)
			if !hasEmptyCell(cells) {
				return cells, false, true, nil
			}
		}
	}
	if found {
		return cells, false, true, nil
	}
	return nil, false, false, nil
}

// loadGranuleFull decodes every column's cells of granule g, indexed from 0
// within the granule, returning per-row cell slices (v4 parts). This mirrors
// loadRows but for a single granule.
func (p *mpart) loadGranuleFull(g int) ([][][]byte, error) {
	if g < 0 || g >= len(p.granules) {
		return nil, nil
	}
	lo, hi := p.granuleRowRange(g)
	full := make([][][]byte, hi-lo)
	for c := 0; c < p.ncols; c++ {
		cells, err := p.loadGranuleCol(g, c)
		if err != nil {
			return nil, err
		}
		for i := range full {
			if i < len(cells) {
				full[i] = append(full[i], cells[i])
			}
		}
	}
	return full, nil
}

// lookupCol resolves a single column cell of key through the merged view,
// loading only that column's part file (point reads should not decompress the
// whole part). Returns the cell (nil if absent or deleted). A partial newest
// version whose cell for this column is empty inherits from the next version.
func (t *mpartTx) lookupCol(table string, colIdx int, key []byte) ([]byte, error) {
	if ov := t.overlay[table]; ov != nil {
		if r, ok := ov.rows[string(key)]; ok {
			if r.del {
				return nil, nil
			}
			if colIdx >= 0 && colIdx < len(r.cells) && len(r.cells[colIdx]) > 0 {
				return r.cells[colIdx], nil
			}
		}
	}
	if t.cleared[table] {
		return nil, nil
	}
	parts, mem := t.committedView(table)
	if mem != nil {
		if r, ok := mem.rows[string(key)]; ok {
			if r.del {
				return nil, nil
			}
			if colIdx >= 0 && colIdx < len(r.cells) && len(r.cells[colIdx]) > 0 {
				return r.cells[colIdx], nil
			}
		}
	}
	for i := len(parts) - 1; i >= 0; i-- {
		p := parts[i]
		if !p.bloom.mayContain(key) {
			continue
		}
		if g := p.granuleFor(key); g >= 0 {
			// v4 part: decode only the granule that can hold the key.
			pks, gerr := p.loadGranulePks(g)
			if gerr != nil {
				return nil, gerr
			}
			gi := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
			if gi < len(pks) && bytes.Equal(pks[gi], key) {
				dels, derr := p.loadGranuleDels(g)
				if derr != nil {
					return nil, derr
				}
				if dels[gi] {
					return nil, nil
				}
				cells, cerr := p.loadGranuleCol(g, colIdx)
				if cerr != nil {
					return nil, cerr
				}
				if gi < len(cells) && len(cells[gi]) > 0 {
					return cells[gi], nil
				}
			}
			continue
		}
		if len(p.granules) > 0 {
			// v4 part but the key lies outside the part's PK span (a bloom
			// false positive): skip without decompressing.
			continue
		}
		pks, cells, lerr := p.loadCol(colIdx)
		if lerr != nil {
			return nil, lerr
		}
		dels, derr := p.loadDels()
		if derr != nil {
			return nil, derr
		}
		i := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
		if i < len(pks) && bytes.Equal(pks[i], key) {
			if dels[i] {
				return nil, nil
			}
			if i < len(cells) && len(cells[i]) > 0 {
				return cells[i], nil
			}
		}
	}
	return nil, nil
}

// rowEach yields every live row of table in PK order with full cell blobs.
func (t *mpartTx) rowEach(table string, fn func(k, v []byte) error) error {
	entries, err := t.collectEntries(table, -1, true, nil, nil)
	if err != nil {
		return err
	}
	for i := 0; i < len(entries); {
		e := entries[i]
		for i+1 < len(entries) && bytes.Equal(entries[i+1].pk, e.pk) {
			i++
		}
		if !e.del && len(e.cells) > 0 {
			if err := fn(e.pk, joinRow(e.cells)); err != nil {
				return err
			}
		}
		i++
	}
	return nil
}

// rowDeleteAll marks the table as cleared: reads see it empty and the next
// commit drops its parts and mem wholesale (rows re-added in the same tx land
// in the overlay and survive).
func (t *mpartTx) rowDeleteAll(table string) error {
	if t.cleared == nil {
		t.cleared = map[string]bool{}
	}
	t.cleared[table] = true
	delete(t.overlay, table)
	if t.deltas != nil {
		delete(t.deltas, table)
	}
	return nil
}

func (t *mpartTx) dropTable(name string) error {
	if err := t.schemaDelete(name); err != nil {
		return err
	}
	return t.rowDeleteAll(name)
}

func (t *mpartTx) commit() error {
	s := t.s
	if len(t.overlay) == 0 && len(t.cleared) == 0 && len(t.deltas) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for tbl := range t.cleared {
		for _, p := range s.parts[tbl] {
			s.retirePart(p)
		}
		s.parts[tbl] = nil
		s.mem[tbl] = nil
		s.counts[tbl] = 0
	}
	for tbl, mp := range t.overlay {
		cur := s.mem[tbl]
		if cur == nil {
			cur = &memPart{rows: map[string]*memRow{}}
			s.mem[tbl] = cur
		}
		t0 := time.Now()
		foldedIntoEmpty := len(cur.rows) == 0
		for k, r := range mp.rows {
			// CH partial-merge: an overlay row may be partial (empty cells for
			// untouched columns). When it overwrites a full mem row, inherit the
			// missing columns from that row right here, so a flush can never
			// persist a partial part whose full version was only in mem.
			// Copy before filling: batch writers (the constant-range UPDATE)
			// share one immutable cell slice across every row they rewrite,
			// so stored cells must never be mutated in place.
			if old, ok := cur.rows[k]; ok && old != r && !old.del {
				merged := make([][]byte, len(r.cells))
				copy(merged, r.cells)
				fillEmptyCells(merged, old.cells)
				r.cells = merged
			}
			cur.rows[k] = r
		}
		// Carry the writer's insertion-order hint only when this tx folded a
		// duplicate-free batch into an empty mem part: then the hint is exactly
		// the folded row set in ascending PK order (sorted scan output).
		if foldedIntoEmpty && mp.hint != nil && len(mp.hint) == len(mp.rows) && memRowsSorted(mp.hint) {
			cur.hint = mp.hint
		} else {
			cur.hint = nil
		}
		cur.gen++
		s.lastWrite[tbl] = time.Now()
		foldDur := time.Since(t0)
		if len(cur.rows) >= mpartFlushThreshold {
			t1 := time.Now()
			if err := s.flushLocked(tbl); err != nil {
				return err
			}
			if d := time.Since(t1); d > 150*time.Millisecond || foldDur > 150*time.Millisecond {
				log.Printf("MPART-COMMIT-SLOW: fold %v flush %v (%d rows)", foldDur, d, len(cur.rows))
			}
		} else if foldDur > 150*time.Millisecond {
			log.Printf("MPART-COMMIT-SLOW: fold %v (no flush)", foldDur)
		}
	}
	for tbl, d := range t.deltas {
		s.counts[tbl] += d
	}
	return nil
}

func (t *mpartTx) rollback() error {
	s := t.s
	if t.snap {
		s.mu.Lock()
		s.snapRefs--
		if s.snapRefs <= 0 && len(s.trash) > 0 {
			for _, tr := range s.trash {
				os.RemoveAll(tr)
			}
			s.trash = nil
		}
		s.mu.Unlock()
	}
	return nil
}

// countFor reports the live row count of table including this tx's deltas.
func (t *mpartTx) countFor(table string) (int64, error) {
	if t.cleared[table] {
		return t.deltas[table], nil
	}
	if t.snap {
		return t.snapCount[table] + t.deltas[table], nil
	}
	s := t.s
	s.mu.Lock()
	base := s.counts[table]
	s.mu.Unlock()
	return base + t.deltas[table], nil
}

// Merge precedence: the in-flight overlay is newest, then committed mem (a
// flush empties mem, so every mem row is newer than every part), then on-disk
// parts ordered by creation seq (newer part wins on PK overlap, including
// tombstones).
const (
	prioOverlay = 1 << 62
	prioMem     = prioOverlay - 1
)

// srcRow is one row from a single source (part/mem/overlay) before the merge
// resolves newest-writer-wins. cell is the single-column view, cells the
// full-width view (mutually exclusive).
type srcRow struct {
	pk    []byte
	cell  []byte
	cells [][]byte
	del   bool
	prio  int
}

// mergeSrc is one sorted source run (a part, or a pk-sorted slice of
// mem/overlay rows) fed into the k-way merge below. Sources are trimmed to the
// query window and iterate in pk order: ascending (step +1) or, when rev,
// descending (step -1).
type mergeSrc struct {
	pks   [][]byte
	cells [][]byte   // single-column view when colIdx >= 0
	fulls [][][]byte // full-width view when full
	dels  []bool
	prio  int
	i     int // current position
	step  int // +1 ascending, -1 descending
	last  int // position at which iteration stops (exclusive)

	// fromPart/denseCol mark a source loaded from an on-disk part whose
	// requested column is in the dense fixed-width layout (no empty cells).
	// They gate the covering-newest-source fast path below.
	fromPart bool
	denseCol bool

	// vals/nulls hold a dense fixed-width numeric column (single-column view,
	// parallel to pks) when the source was decoded from a dense part column.
	vals  []int64
	nulls []uint64
}

func (s *mergeSrc) done() bool { return s.i == s.last }

// mergeHeap orders sources by (pk, prio desc): for a shared pk the newest
// (highest-priority) row pops first. With rev set, pk order is reversed so the
// largest pk pops first (used by ORDER BY ... DESC).
type mergeHeap struct {
	rev bool
	h   []*mergeSrc
}

func (m *mergeHeap) Len() int { return len(m.h) }

func (m *mergeHeap) Less(i, j int) bool {
	c := bytes.Compare(m.h[i].pks[m.h[i].i], m.h[j].pks[m.h[j].i])
	if c != 0 {
		if m.rev {
			return c > 0
		}
		return c < 0
	}
	return m.h[i].prio > m.h[j].prio
}

func (m *mergeHeap) Swap(i, j int) { m.h[i], m.h[j] = m.h[j], m.h[i] }
func (m *mergeHeap) Push(x any) {
	m.h = append(m.h, x.(*mergeSrc))
}
func (m *mergeHeap) Pop() any {
	old := m.h
	n := len(old)
	x := old[n-1]
	m.h = old[:n-1]
	return x
}

// sliceRange computes the [start, end) index window of a pk-sorted array that
// falls inside [plLower, plUpper) (nil = unbounded).
func sliceRange(pks [][]byte, plLower, plUpper []byte) (int, int) {
	n := len(pks)
	start, end := 0, n
	if plLower != nil {
		start = sort.Search(n, func(i int) bool { return bytes.Compare(pks[i], plLower) >= 0 })
	}
	if plUpper != nil {
		end = sort.Search(n, func(i int) bool { return bytes.Compare(pks[i], plUpper) >= 0 })
	}
	if start > end {
		return 0, 0
	}
	return start, end
}

// coveringNewestSource returns the newest merge source when it provably holds
// every key of the half-open range (plLower, plUpper both non-nil) and its
// single-column view is dense, so reading it alone yields exactly the merged
// result. Coverage is proven only for contiguous single-int64 PKs: sorted
// unique keys with first == lower, last == upper-1 and count == upper-lower
// leave no room for a gap an older part could fill. Any mem/overlay presence
// disqualifies automatically — their prio always exceeds a part's seq.
func coveringNewestSource(srcs []*mergeSrc, plLower, plUpper []byte) *mergeSrc {
	if len(srcs) < 2 || plLower == nil || plUpper == nil ||
		len(plLower) != 10 || len(plUpper) != 10 {
		return nil
	}
	best := -1
	for i, s := range srcs {
		if best < 0 || s.prio > srcs[best].prio {
			best = i
		}
	}
	s := srcs[best]
	if !s.fromPart {
		// mem/overlay source: no format metadata, so density of the window is
		// verified directly — every cell must be non-empty (a single empty
		// cell would need inheritance from an older version).
		if s.cells == nil {
			return nil
		}
		for k := s.i; k < s.last; k++ {
			if len(s.cells[k]) == 0 {
				return nil
			}
		}
	}
	pks := s.pks
	if len(pks) < 1 || len(pks[0]) != 10 {
		return nil
	}
	first := binary.BigEndian.Uint64(pks[0][2:10])
	last := binary.BigEndian.Uint64(pks[len(pks)-1][2:10])
	lower := binary.BigEndian.Uint64(plLower[2:10])
	upper := binary.BigEndian.Uint64(plUpper[2:10])
	if first != lower || last != upper-1 || uint64(len(pks)) != upper-lower {
		return nil
	}
	return s
}

func mapRows(mp *memPart) int {
	if mp == nil {
		return 0
	}
	return len(mp.rows)
}

// walkMergedLocked streams the table's rows inside [plLower, plUpper) in PK
// order (reverse if rev), resolving newest-writer-wins and invoking fn once
// per live-or-deleted PK group. Caller holds the store lock.
// walkPksDescGranules yields every live pk in descending PK order for the
// given parts, walking granule blocks from the largest pk down. It returns
// (handled=true, err) when it fully owned the walk (all parts granule-aligned
// and pairwise-disjoint in PK); handled=false means the caller must fall back
// to the full merge. err propagates the callback's abort (errStop for ORDER
// BY ... DESC LIMIT, in which case only the tail granules were decoded).
// DEBUG
func (t *mpartTx) walkPksDescGranules(parts []*mpart, fn func(pk []byte, cell []byte, cells [][]byte, del bool) error) (bool, error) {
	var granular []*mpart
	for _, p := range parts {
		if len(p.granules) == 0 {
			return false, nil
		}
		granular = append(granular, p)
	}
	if len(granular) == 0 {
		return false, nil
	}
	// Pairwise-disjoint PK windows are required: with disjoint parts the pk
	// order across parts is well-defined (no newest-wins resolution needed).
	sort.Slice(granular, func(i, j int) bool { return bytes.Compare(granular[i].pkMin, granular[j].pkMin) < 0 })
	for k := 0; k < len(granular)-1; k++ {
		if bytes.Compare(granular[k].pkMax, granular[k+1].pkMin) >= 0 {
			return false, nil
		}
	}
	for i := len(granular) - 1; i >= 0; i-- {
		p := granular[i]
		for g := len(p.granules) - 1; g >= 0; g-- {
			pks, err := p.loadGranulePks(g)
			if err != nil {
				return false, err
			}
			dels, err := p.loadGranuleDels(g)
			if err != nil {
				return false, err
			}
			for r := len(pks) - 1; r >= 0; r-- {
				if err := fn(pks[r], nil, nil, dels[r]); err != nil {
					return true, err // errStop: abort the walk.
				}
			}
		}
	}
	return true, nil
}

func (t *mpartTx) walkMergedLocked(table string, colIdx int, full bool, rev bool, plLower, plUpper []byte, fn func(pk []byte, cell []byte, cells [][]byte, del bool) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range matches nothing
	}
	if !full && colIdx < 0 && rev && plLower == nil && plUpper == nil && !t.cleared[table] && t.overlay[table] == nil {
		// ORDER BY pk DESC: walk only the granules that can still contribute
		// keys, newest part first. The callback stops via errStop once the
		// limit is reached, so a LIMIT read decodes just the tail granules
		// (ClickHouse sparse-index reverse scan) instead of the whole part.
		parts, mem := t.committedViewLocked(table)
		if mapRows(mem) == 0 {
			if handled, werr := t.walkPksDescGranules(parts, fn); handled {
				return werr
			}
		}
	}
	var h mergeHeap
	h.rev = rev
	addPart := func(p *mpart, pks [][]byte, cells [][]byte, fulls [][][]byte, dels []bool) {
		lo, hi := sliceRange(pks, plLower, plUpper)
		if lo >= hi {
			return
		}
		s := &mergeSrc{pks: pks, cells: cells, fulls: fulls, dels: dels, prio: p.seq,
			fromPart: true,
			denseCol: colIdx >= 0 && !full && colIdx < len(p.colFmts) && p.colFmts[colIdx] != colFmtLegacy,
		}
		if rev {
			s.i = hi - 1
			s.last = lo - 1
			s.step = -1
		} else {
			s.i = lo
			s.last = hi
			s.step = 1
		}
		h.h = append(h.h, s)
	}
	if !t.cleared[table] {
		parts, mem := t.committedViewLocked(table)
		for _, p := range parts {
			if !p.inRange(plLower, plUpper) {
				continue
			}
			var pks [][]byte
			var cells [][]byte
			var fulls [][][]byte
			var err error
			switch {
			case full:
				pks, fulls, err = p.loadRows()
			case colIdx >= 0:
				pks, cells, err = p.loadCol(colIdx)
			default:
				pks, err = p.loadPks()
			}
			if err != nil {
				return err
			}
			dels, err := p.loadDels()
			if err != nil {
				return err
			}
			addPart(p, pks, cells, fulls, dels)
		}
		if mem != nil {
			if err := addMemSource(&h, mem, colIdx, full, rev, plLower, plUpper); err != nil {
				return err
			}
		}
	}
	if ov := t.overlay[table]; ov != nil {
		if err := addRowSource(&h, ov.rows, colIdx, full, rev, prioOverlay, plLower, plUpper); err != nil {
			return err
		}
	}
	if len(h.h) == 0 {
		return nil
	}
	// Fast paths: most reads hit a single part (point queries, narrow ranges)
	// or several parts with pairwise-disjoint PK windows (sequential inserts
	// flush to non-overlapping parts). In both cases the heap is pure overhead:
	// no two sources can shadow each other, so iterate the slices directly in
	// merge order (ClickHouse concatenates granule streams the same way).
	if len(h.h) == 1 {
		return walkSource(h.h[0], full, fn)
	}
	// Covering-newest fast path: after a range UPDATE every rewritten key of
	// [plLower, plUpper) lives in the newest flushed part (the batch rewrote
	// exactly the live rows it scanned), so within that window every older
	// part is fully shadowed. When the newest source provably holds *every*
	// key of the range (contiguous int64 PKs) and its column is dense (no
	// empty cells to inherit), walking it alone is exact — the k-way heap and
	// the older parts' column loads are pure overhead.
	if !rev {
		if s := coveringNewestSource(h.h, plLower, plUpper); s != nil {
			return walkSource(s, full, fn)
		}
	}
	if sortDisjointSources(h.h, rev) {
		for _, s := range h.h {
			if err := walkSource(s, full, fn); err != nil {
				return err
			}
		}
		return nil
	}
	heap.Init(&h)
	for h.Len() > 0 {
		s := heap.Pop(&h).(*mergeSrc)
		i := s.i
		pk := s.pks[i]
		s.i += s.step
		if !s.done() {
			heap.Push(&h, s)
		}
		if full {
			cells := s.fulls[i]
			if !s.dels[i] && hasEmptyCell(cells) {
				// CH partial-merge: the newest version is a partial row (its
				// untouched columns are empty). Copy it and inherit each missing
				// column from the older shadowed versions (at the heap head,
				// same pk, lower prio), advancing them as we go.
				merged := make([][]byte, len(cells))
				copy(merged, cells)
				for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) && hasEmptyCell(merged) {
					top := h.h[0]
					if !top.dels[top.i] {
						fillEmptyCells(merged, top.fulls[top.i])
					}
					top.i += top.step
					if top.done() {
						heap.Pop(&h)
					} else {
						heap.Fix(&h, 0)
					}
				}
				cells = merged
			}
			// Advance every other source whose head pk is the same: with the
			// heap's (pk, prio desc) order these are older versions shadowed by
			// s. (Reached either after the column-wise inherit loop consumed
			// them, or directly when s is a full row / tombstone.)
			for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) {
				top := h.h[0]
				top.i += top.step
				if top.done() {
					heap.Pop(&h)
				} else {
					heap.Fix(&h, 0)
				}
			}
			if err := fn(pk, nil, cells, s.dels[i]); err != nil {
				return err
			}
		} else if colIdx >= 0 && s.cells != nil {
			cell := s.cells[i]
			if !s.dels[i] && len(cell) == 0 {
				// Column-wise inheritance for a single-column view: the newest
				// version lacks this column, so take it from the next non-empty
				// older version.
				for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) {
					top := h.h[0]
					if len(cell) == 0 && !top.dels[top.i] {
						cell = top.cells[top.i]
					}
					top.i += top.step
					if top.done() {
						heap.Pop(&h)
					} else {
						heap.Fix(&h, 0)
					}
				}
			} else {
				for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) {
					top := h.h[0]
					top.i += top.step
					if top.done() {
						heap.Pop(&h)
					} else {
						heap.Fix(&h, 0)
					}
				}
			}
			if err := fn(pk, cell, nil, s.dels[i]); err != nil {
				return err
			}
		} else {
			// PK-only walk: no cell data; newest wins outright.
			for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) {
				top := h.h[0]
				top.i += top.step
				if top.done() {
					heap.Pop(&h)
				} else {
					heap.Fix(&h, 0)
				}
			}
			if err := fn(pk, nil, nil, s.dels[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// addRowSource builds a pk-sorted source run from a mem/overlay row map and
// appends it to the merge heap, honoring the query direction.
func addRowSource(h *mergeHeap, rows map[string]*memRow, colIdx int, full bool, rev bool, prio int, plLower, plUpper []byte) error {
	var rs []*memRow
	for _, r := range rows {
		if inPKRange(r.pk, plLower, plUpper) {
			rs = append(rs, r)
		}
	}
	if len(rs) == 0 {
		return nil
	}
	sort.Slice(rs, func(i, j int) bool { return bytes.Compare(rs[i].pk, rs[j].pk) < 0 })
	pks := make([][]byte, len(rs))
	dels := make([]bool, len(rs))
	var cells [][]byte
	var fulls [][][]byte
	if full {
		fulls = make([][][]byte, len(rs))
		for i, r := range rs {
			pks[i] = r.pk
			fulls[i] = r.cells
			dels[i] = r.del
		}
	} else if colIdx >= 0 {
		cells = make([][]byte, len(rs))
		for i, r := range rs {
			pks[i] = r.pk
			dels[i] = r.del
			if colIdx < len(r.cells) {
				cells[i] = r.cells[colIdx]
			}
		}
	} else {
		for i, r := range rs {
			pks[i] = r.pk
			dels[i] = r.del
		}
	}
	lo, hi := sliceRange(pks, plLower, plUpper)
	if lo >= hi {
		return nil
	}
	s := &mergeSrc{pks: pks, cells: cells, fulls: fulls, dels: dels, prio: prio}
	if rev {
		s.i = hi - 1
		s.last = lo - 1
		s.step = -1
	} else {
		s.i = lo
		s.last = hi
		s.step = 1
	}
	h.h = append(h.h, s)
	return nil
}

// ensureCached builds (or reuses) the mem part's pk-sorted view. Mem rows are
// immutable and only commit() appends, so a view built at gen stays valid until
// the next commit bumps gen.
func (mp *memPart) ensureCached() {
	if mp.gen == mp.cacheGen && mp.cacheRows != nil {
		return
	}
	var rs []*memRow
	if mp.hint != nil && len(mp.hint) == len(mp.rows) && memRowsSorted(mp.hint) {
		// The writer guaranteed ascending PK order (sorted UPDATE scan); skip
		// both the map gather and the sort.
		rs = mp.hint
	} else {
		rs = make([]*memRow, 0, len(mp.rows))
		for _, r := range mp.rows {
			rs = append(rs, r)
		}
		sort.Slice(rs, func(i, j int) bool { return bytes.Compare(rs[i].pk, rs[j].pk) < 0 })
	}
	pks := make([][]byte, len(rs))
	dels := make([]bool, len(rs))
	for i, r := range rs {
		pks[i] = r.pk
		dels[i] = r.del
	}
	mp.cacheRows = rs
	mp.cachePks = pks
	mp.cacheDels = dels
	mp.cacheCells = map[int][][]byte{}
	mp.cacheFulls = nil
	mp.cacheGen = mp.gen
}

// addMemSource builds a pk-sorted source run from a mem part, reusing the
// cached decoded view when the mem part has not changed since it was built
// (mem rows are immutable; only commit() appends to the map, bumping gen).
// This is the ClickHouse "decode once, read many" pattern for the in-memory
// insert buffer: a stable trailing tail costs one sort + one cell pass instead
// of re-doing both on every read.
func addMemSource(h *mergeHeap, mp *memPart, colIdx int, full bool, rev bool, plLower, plUpper []byte) error {
	mp.ensureCached()
	var cells [][]byte
	var fulls [][][]byte
	if full {
		if mp.cacheFulls == nil {
			fulls = make([][][]byte, len(mp.cacheRows))
			for i, r := range mp.cacheRows {
				fulls[i] = r.cells
			}
			mp.cacheFulls = fulls
		} else {
			fulls = mp.cacheFulls
		}
	} else if colIdx >= 0 {
		c, ok := mp.cacheCells[colIdx]
		if !ok {
			c = make([][]byte, len(mp.cacheRows))
			for i, r := range mp.cacheRows {
				if colIdx < len(r.cells) {
					c[i] = r.cells[colIdx]
				}
			}
			mp.cacheCells[colIdx] = c
		}
		cells = c
	}
	pks, dels := mp.cachePks, mp.cacheDels
	lo, hi := sliceRange(pks, plLower, plUpper)
	if lo >= hi {
		return nil
	}
	s := &mergeSrc{pks: pks, cells: cells, fulls: fulls, dels: dels, prio: prioMem}
	if rev {
		s.i = hi - 1
		s.last = lo - 1
		s.step = -1
	} else {
		s.i = lo
		s.last = hi
		s.step = 1
	}
	h.h = append(h.h, s)
	return nil
}

// walkSource iterates one merge source from its current position to last,
// invoking fn once per row. Single-source fast path: no version shadowing is
// possible, so the heap machinery is skipped entirely.
func walkSource(s *mergeSrc, full bool, fn func(pk []byte, cell []byte, cells [][]byte, del bool) error) error {
	for i := s.i; i != s.last; i += s.step {
		var cell []byte
		var cells [][]byte
		if full {
			cells = s.fulls[i]
		} else if s.cells != nil {
			cell = s.cells[i]
		}
		if err := fn(s.pks[i], cell, cells, s.dels[i]); err != nil {
			return err
		}
	}
	return nil
}

// hasEmptyCell reports whether the row's cell slice contains an empty (len-0)
// blob. A len-0 cell is the CH "column not touched in this version" marker
// (distinct from NULL, which is a [type][null=1] variant, 2+ bytes): partial
// UPDATE rows write only their changed columns and leave the rest empty so the
// column-wise merge below inherits them from the previous version.
func hasEmptyCell(cells [][]byte) bool {
	for _, c := range cells {
		if len(c) == 0 {
			return true
		}
	}
	return false
}

// fillEmptyCells overwrites the empty (len-0) slots of dst with the
// corresponding non-empty cells of src (column-wise inheritance). src may be a
// shorter row (a tombstone-only part has no column cells), so reads are bounds
// checked; tombstone (del) rows carry no cells and contribute nothing.
func fillEmptyCells(dst, src [][]byte) {
	for c := range dst {
		if len(dst[c]) == 0 && c < len(src) && len(src[c]) > 0 {
			dst[c] = src[c]
		}
	}
}

// sortDisjointSources sorts the sources by their first in-range PK (desc if
// rev) and returns true when they occupy pairwise-disjoint PK windows, in
// which case no pk appears in two sources and the merge degenerates to a
// concatenation. When it returns true the sources are left sorted so callers
// iterate them sequentially.
func sortDisjointSources(ss []*mergeSrc, rev bool) bool {
	first := func(s *mergeSrc) []byte { return s.pks[s.i] }
	if rev {
		sort.Slice(ss, func(i, j int) bool { return bytes.Compare(first(ss[i]), first(ss[j])) > 0 })
		for k := 0; k < len(ss)-1; k++ {
			// In rev mode each source iterates hi-1 .. lo; its smallest pk is
			// at index last+1 (last = lo-1 is the loop stop sentinel).
			if bytes.Compare(ss[k].pks[ss[k].last+1], ss[k+1].pks[ss[k+1].i]) <= 0 {
				return false
			}
		}
		return true
	}
	sort.Slice(ss, func(i, j int) bool { return bytes.Compare(first(ss[i]), first(ss[j])) < 0 })
	for k := 0; k < len(ss)-1; k++ {
		if bytes.Compare(ss[k].pks[ss[k].last-1], ss[k+1].pks[ss[k+1].i]) >= 0 {
			return false
		}
	}
	return true
}

// collectEntries gathers every row of table inside [plLower, plUpper) from all
// sources, resolving to the newest live-or-deleted version per PK, sorted by
// PK ascending. colIdx >= 0 loads only that column's cell; colIdx < 0 with
// full=false loads PKs only; full=true loads every column.
func (t *mpartTx) collectEntries(table string, colIdx int, full bool, plLower, plUpper []byte) ([]srcRow, error) {
	if t.snap {
		return t.collectEntriesLocked(table, colIdx, full, plLower, plUpper)
	}
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.collectEntriesLocked(table, colIdx, full, plLower, plUpper)
}

// walkMerged streams the table's rows inside [plLower, plUpper) in PK order
// (reverse if rev), resolving newest-writer-wins and invoking fn once per
// live-or-deleted PK group. It acquires the store lock unless t is a snapshot.
func (t *mpartTx) walkMerged(table string, colIdx int, full bool, rev bool, plLower, plUpper []byte, fn func(pk []byte, cell []byte, cells [][]byte, del bool) error) error {
	if t.snap {
		return t.walkMergedLocked(table, colIdx, full, rev, plLower, plUpper, fn)
	}
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.walkMergedLocked(table, colIdx, full, rev, plLower, plUpper, fn)
}

// collectEntriesLocked is collectEntries with the store lock already held.
func (t *mpartTx) collectEntriesLocked(table string, colIdx int, full bool, plLower, plUpper []byte) ([]srcRow, error) {
	var out []srcRow
	err := t.walkMergedLocked(table, colIdx, full, false, plLower, plUpper, func(pk, cell []byte, cells [][]byte, del bool) error {
		r := srcRow{pk: pk, del: del}
		if full {
			r.cells = cells
		} else if colIdx >= 0 {
			r.cell = cell
		}
		out = append(out, r)
		return nil
	})
	return out, err
}

// walkGrouped iterates merged entries invoking fn once per live PK group with
// the highest-priority (newest) row; deleted groups are skipped.
func walkGrouped(entries []srcRow, fn func(e srcRow) error) error {
	for i := 0; i < len(entries); {
		e := entries[i]
		for i+1 < len(entries) && bytes.Equal(entries[i+1].pk, e.pk) {
			i++
		}
		if !e.del {
			if err := fn(e); err != nil {
				return err
			}
		}
		i++
	}
	return nil
}

// compactTable merges the table's parts into fresh parts, dropping tombstoned
// rows and superseded versions, and reclaims the old part files. It returns
// the number of live rows after compaction.
//
// The heavy work (reading the immutable parts and writing the fresh ones)
// happens WITHOUT the store lock: parts are immutable once written, so a
// large compaction never stalls concurrent reads on s.mu. The swap at the end
// is atomic under the lock.
func (s *mpartStore) compactTable(tbl string) (int64, error) {
	// Phase 1 (under lock): snapshot the parts to merge and the mem rows.
	// Memory rows are kept by pointer so the final swap can drop exactly the
	// rows that were folded into the fresh parts (any row overwritten by a
	// commit during the merge has a different pointer and is preserved).
	s.mu.Lock()
	oldParts := append([]*mpart(nil), s.parts[tbl]...)
	mem := s.mem[tbl]
	oldCount := s.counts[tbl]
	memRows := map[string]*memRow{}
	memSnap := &memPart{rows: memRows}
	if mem != nil {
		for k, r := range mem.rows {
			memRows[k] = r
		}
	}
	s.mu.Unlock()

	// Phase 2 (no lock): read the snapshot parts + mem, resolve newest-wins,
	// and write the fresh parts. The snapshot tx never takes s.mu (parts are
	// immutable; the mem rows are frozen copies by pointer).
	t := &mpartTx{s: s, snap: true,
		snapParts: map[string][]*mpart{tbl: oldParts},
		snapMem:   map[string]*memPart{tbl: memSnap},
		snapCount: map[string]int64{tbl: oldCount},
	}
	entries, err := t.collectEntries(tbl, -1, true, nil, nil)
	if err != nil {
		return 0, err
	}
	rows := make([]*memRow, 0, len(entries))
	for i := 0; i < len(entries); {
		e := entries[i]
		for i+1 < len(entries) && bytes.Equal(entries[i+1].pk, e.pk) {
			i++
		}
		if !e.del && len(e.cells) > 0 {
			rows = append(rows, &memRow{pk: e.pk, cells: e.cells})
		}
		i++
	}
	// Reserve a contiguous id range under the lock so concurrent flushes can't
	// collide, then write the chunks outside it.
	nchunks := (len(rows) + mpartMergeChunk - 1) / mpartMergeChunk
	s.mu.Lock()
	startID := s.nextID + 1
	s.nextID += nchunks
	s.mu.Unlock()
	newParts := make([]*mpart, 0, nchunks)
	for i := 0; i < nchunks; i++ {
		a := i * mpartMergeChunk
		b := a + mpartMergeChunk
		if b > len(rows) {
			b = len(rows)
		}
		p, err := s.writePart(tbl, rows[a:b], startID+i)
		if err != nil {
			return 0, err
		}
		newParts = append(newParts, p)
	}

	// Phase 3 (under lock): atomically swap the fresh parts in, drop the old
	// ones, and adjust the live count for commits that landed mid-merge.
	s.mu.Lock()
	delta := s.counts[tbl] - oldCount
	if cur := s.mem[tbl]; cur != nil {
		for k, r := range memRows {
			if cur.rows[k] == r {
				delete(cur.rows, k)
			}
		}
	}
	oldSet := make(map[*mpart]bool, len(oldParts))
	for _, p := range oldParts {
		oldSet[p] = true
	}
	kept := make([]*mpart, 0, len(s.parts[tbl]))
	for _, p := range s.parts[tbl] {
		if !oldSet[p] {
			kept = append(kept, p)
		}
	}
	s.parts[tbl] = append(kept, newParts...)
	sortPartsBySeq(s.parts[tbl])
	s.counts[tbl] = int64(len(rows)) + delta
	for _, p := range oldParts {
		s.retirePart(p)
	}
	s.mu.Unlock()
	return s.counts[tbl], nil
}

// --- frame + compression helpers ---

// encodeFrames packs [][]byte blobs into one byte slice as uvarint-length-
// prefixed frames.
func encodeFrames(blobs [][]byte) []byte {
	var out []byte
	for _, b := range blobs {
		var l [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(l[:], uint64(len(b)))
		out = append(out, l[:n]...)
		out = append(out, b...)
	}
	return out
}

func decodeFrames(raw []byte) [][]byte {
	var out [][]byte
	for len(raw) > 0 {
		l, n := binary.Uvarint(raw)
		if n <= 0 || uint64(len(raw)-n) < l {
			break
		}
		out = append(out, raw[n:n+int(l)])
		raw = raw[n+int(l):]
	}
	return out
}

// denseToCells rebuilds per-row cell blobs from a dense numeric column in the
// exact [type][null][zigzag varint] encoding the writer produced, so generic
// consumers (full-row scans, point reads, ORDER BY) see identical cells.
func denseToCells(vals []int64, nulls []uint64, t sqlType) [][]byte {
	cells := make([][]byte, len(vals))
	var v sqlValue
	v.typ = t
	for i, val := range vals {
		v.null = nulls[i>>6]&(1<<(i&63)) != 0
		switch t {
		case tFloat:
			v.f = math.Float64frombits(uint64(val))
		case tInt, tTimestamp:
			v.i = val
		}
		b := makeBuilder()
		b.Variant(v)
		cells[i] = b.Bytes()
	}
	return cells
}

// denseColumnType reports whether every cell blob in the column is a
// well-formed numeric cell of a single type. Cells are encoded as
// [type][null][zigzag varint] (see codec.builder.Variant). A cell shaped
// differently (string, bool, legacy junk) forces the legacy frames layout.
func denseColumnType(cells [][]byte) (sqlType, bool) {
	if len(cells) == 0 {
		return 0, false
	}
	var t sqlType
	t = -1
	for _, cell := range cells {
		if len(cell) < 2 {
			return 0, false
		}
		ct := sqlType(cell[0])
		switch ct {
		case tInt, tFloat, tTimestamp:
		default:
			return 0, false
		}
		if t == -1 {
			t = ct
		} else if t != ct {
			return 0, false
		}
		if cell[1] != 0 {
			continue // null cell: value bytes irrelevant
		}
		if _, n := binary.Uvarint(cell[2:]); n <= 0 {
			return 0, false
		}
	}
	return t, t >= 0
}

// encodeDenseNumeric packs a numeric column into the fixed-width layout:
// [uvarint nrows][null bitmap ceil(nrows/8) bytes][nrows*8 big-endian int64
// values]. Null cells store a zero value and set their bitmap bit. Values for
// tFloat are the float64 bit patterns; for tInt/tTimestamp the integer value.
func encodeDenseNumeric(cells [][]byte, t sqlType) []byte {
	n := len(cells)
	bitmap := (n + 7) / 8
	out := make([]byte, 0, binary.MaxVarintLen64+bitmap+n*8)
	var l [binary.MaxVarintLen64]byte
	ln := binary.PutUvarint(l[:], uint64(n))
	out = append(out, l[:ln]...)
	nullBase := len(out)
	out = append(out, make([]byte, bitmap)...)
	for i, cell := range cells {
		if cell[1] != 0 {
			out[nullBase+i/8] |= 1 << (i & 7)
			out = binary.BigEndian.AppendUint64(out, 0)
			continue
		}
		u, _ := binary.Uvarint(cell[2:])
		v := int64(u>>1) ^ -int64(u&1) // zigzag decode: value, or float64 bits
		out = binary.BigEndian.AppendUint64(out, uint64(v))
	}
	return out
}

// decodeDenseNumeric unpacks the fixed-width layout into values + null bitmap.
func decodeDenseNumeric(raw []byte) ([]int64, []uint64, error) {
	n, nl := binary.Uvarint(raw)
	if nl <= 0 {
		return nil, nil, errors.New("dense: bad row count")
	}
	raw = raw[nl:]
	bitmap := (int(n) + 7) / 8
	if len(raw) < bitmap+int(n)*8 {
		return nil, nil, errors.New("dense: short column")
	}
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(binary.BigEndian.Uint64(raw[bitmap+i*8:]))
	}
	nulls := make([]uint64, (int(n)+63)/64)
	for i := 0; i < int(n); i++ {
		if raw[i/8]&(1<<(i&7)) != 0 {
			nulls[i>>6] |= 1 << (i & 63)
		}
	}
	return vals, nulls, nil
}

func mustLZ4(raw []byte) []byte {
	enc, err := lz4Encode(raw)
	if err != nil {
		panic("mpart: lz4 encode: " + err.Error())
	}
	return enc
}

func lz4Encode(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	// Content checksum off: the blocks are already integrity-checked by the
	// part's CRC list (meta.bin), and skipping the stream xxhash halves the
	// decompression cost of cold scans. Readers accept both (the frame flag
	// is self-describing), so old checksummed parts still load.
	if err := w.Apply(lz4.ChecksumOption(false)); err != nil {
		return nil, err
	}
	if _, err := w.Write(raw); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func lz4Expand(enc []byte) ([]byte, error) {
	var buf bytes.Buffer
	r := lz4.NewReader(bytes.NewReader(enc))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeDenseColGranules decodes a v4 part's dense column by decompressing
// every granule block in parallel (blocks are independently compressed) and
// concatenating the results in order. The uncompressed size of a dense block
// is known exactly (bitmap + n*8 + varint header), so the per-block buffer is
// preallocated and never reallocates.
func (p *mpart) decodeDenseColGranules(colIdx int) ([]int64, []uint64, error) {
	raw, err := os.ReadFile(p.colPath(colIdx))
	if err != nil {
		return nil, nil, err
	}
	vals := make([]int64, 0, p.rowcount)
	nulls := make([]uint64, 0, (p.rowcount+63)/64)
	type res struct {
		vals  []int64
		nulls []uint64
		err   error
	}
	out := make([]res, len(p.granules))
	workers := runtime.GOMAXPROCS(0)
	if workers > len(p.granules) {
		workers = len(p.granules)
	}
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	gch := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for g := range gch {
				blk := raw[p.granules[g].colOff[colIdx] : p.granules[g].colOff[colIdx]+p.granules[g].colLen[colIdx]]
				dec, err := expandBlock(blk, p.granules[g].colRaw[colIdx])
				if err != nil {
					out[g].err = err
					continue
				}
				gv, gn, err := decodeDenseNumeric(dec)
				if err != nil {
					out[g].err = err
					continue
				}
				out[g].vals, out[g].nulls = gv, gn
			}
		}()
	}
	for g := range p.granules {
		gch <- g
	}
	close(gch)
	wg.Wait()
	for _, r := range out {
		if r.err != nil {
			return nil, nil, r.err
		}
		vals = append(vals, r.vals...)
		nulls = append(nulls, r.nulls...)
	}
	return vals, nulls, nil
}

// --- bloom filter ---

// mpartBloom is a small on-disk bloom filter per part: 4 bits per element,
// two hash functions. Serialized into meta.bin.
type mpartBloom struct {
	bits []uint64
}

func newMpartBloom(n int) *mpartBloom {
	words := (n*4)/64 + 1
	if words < 1 {
		words = 1
	}
	return &mpartBloom{bits: make([]uint64, words)}
}

func (b *mpartBloom) hashes(pk []byte) (uint64, uint64) {
	var h1, h2 uint64 = 1469598103934665603, 1099511628211
	for _, c := range pk {
		h1 ^= uint64(c)
		h1 *= 1099511628211
		h2 ^= uint64(c)
		h2 *= 1469598103934665603
	}
	m := uint64(len(b.bits) * 64)
	return h1 % m, h2 % m
}

func (b *mpartBloom) add(pk []byte) {
	i1, i2 := b.hashes(pk)
	b.bits[i1>>6] |= 1 << (i1 & 63)
	b.bits[i2>>6] |= 1 << (i2 & 63)
}

func (b *mpartBloom) mayContain(pk []byte) bool {
	i1, i2 := b.hashes(pk)
	return b.bits[i1>>6]&(1<<(i1&63)) != 0 && b.bits[i2>>6]&(1<<(i2&63)) != 0
}

// isColumnarEngine reports whether engine is a columnar storage engine that
// exposes the columnarTx surface (CSTORE and CSTORE2).
func isColumnarEngine(engine string) bool {
	return engine == "CSTORE" || engine == "CSTORE2"
}

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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
)

// mpartFlushThreshold is the mem-part row count that triggers a flush to a
// new immutable part.
const mpartFlushThreshold = 65536

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
	mpartIdxVer   = 2
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
// on Close.
func (s *mpartStore) startIdleFlusher() {
	go func() {
		t := time.NewTicker(mpartIdleFlushInterval / 2)
		defer t.Stop()
		defer close(s.flushDone)
		for {
			select {
			case <-s.stopFlush:
				return
			case now := <-t.C:
				s.flushIdle(now)
				s.mergeIdle(now)
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
		if s.partsOverlap(tbl) {
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
		}
	}
}

// flushIdle flushes every table whose mem part has not received a write for
// mpartIdleFlushInterval and holds at least mpartIdleFlushMinRows rows.
func (s *mpartStore) flushIdle(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
		if err := s.flushLocked(tbl); err != nil {
			// Best-effort: reads are unaffected (mem still holds the rows);
			// the next tick retries. Failure is only logged via the error
			// return; callers with a logger may handle it.
			_ = err
		}
	}
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

// flushLocked writes the current mem part of tbl to a new immutable part and
// resets the mem part. Live counts are unchanged (rows only moved).
func (s *mpartStore) flushLocked(tbl string) error {
	mp := s.mem[tbl]
	if mp == nil || len(mp.rows) == 0 {
		return nil
	}
	rows := make([]*memRow, 0, len(mp.rows))
	for _, r := range mp.rows {
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].pk, rows[j].pk) < 0 })
	p, err := s.writePart(tbl, rows)
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
func (s *mpartStore) writePart(tbl string, rows []*memRow) (*mpart, error) {
	s.nextID++
	id := s.nextID
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
		for c := 0; c < ncols; c++ {
			p.gColsOnce[c] = make([]sync.Once, ng)
			p.gCols[c] = make([][][]byte, ng)
			p.gColsErr[c] = make([]error, ng)
		}
		for c := 0; c < ncols; c++ {
			for g := range p.granules {
				p.granules[g].colOff = append(p.granules[g].colOff, 0)
				p.granules[g].colLen = append(p.granules[g].colLen, 0)
				p.granules[g].colRaw = append(p.granules[g].colRaw, 0)
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
		// Record each granule's PK window (last PK of the block).
		for g := range p.granules {
			lo := g * mpartGranuleRows
			hi := lo + mpartGranuleRows
			if hi > len(rows) {
				hi = len(rows)
			}
			p.granules[g].pkMax = append([]byte(nil), rows[hi-1].pk...)
		}
		granuleRaw = encodeGranuleIndex(p.granules, ncols)
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
// and tiny (one entry per mpartGranuleRows rows).
func encodeGranuleIndex(gs []mpartGranule, ncols int) []byte {
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
	}
	return b.Bytes()
}

func decodeGranuleIndex(raw []byte, ncols int) ([]mpartGranule, error) {
	r := makeReader(raw)
	if r.Str() != mpartIdxMagic {
		return nil, errors.New("bad mpart idx magic")
	}
	ver := r.Var()
	if ver != 1 && ver != 2 {
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
		gs, err := decodeGranuleIndex(idxRaw, p.ncols)
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
		for c := 0; c < p.ncols; c++ {
			p.gColsOnce[c] = make([]sync.Once, len(gs))
			p.gCols[c] = make([][][]byte, len(gs))
			p.gColsErr[c] = make([]error, len(gs))
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
		return pks, nil, nil
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
		p.denseOnce = make([]sync.Once, p.ncols)
		p.denseVals = make([][]int64, p.ncols)
		p.denseNulls = make([][]uint64, p.ncols)
		p.denseErr = make([]error, p.ncols)
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
	// A row that becomes live raises the table's count; overwriting a live row
	// does not. The existence read goes through the overlay so repeated writes
	// of one pk in a single tx are exact.
	exists, err := t.rowExists(table, key)
	if err != nil {
		return err
	}
	if !exists {
		t.delta(table, 1)
	}
	t.overlayPart(table).rows[string(key)] = &memRow{
		pk:    append([]byte(nil), key...),
		cells: cells,
	}
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
	_, del, found, err := t.lookup(table, key)
	if err != nil {
		return false, err
	}
	return found && !del, nil
}

// lookup resolves key to its newest live-or-deleted row: overlay first, then
// committed mem, then parts (via bloom + binary search).
func (t *mpartTx) lookup(table string, key []byte) (cells [][]byte, del, found bool, err error) {
	if ov := t.overlay[table]; ov != nil {
		if r, ok := ov.rows[string(key)]; ok {
			return r.cells, r.del, true, nil
		}
	}
	if t.cleared[table] {
		return nil, false, false, nil
	}
	parts, mem := t.committedView(table)
	if mem != nil {
		if r, ok := mem.rows[string(key)]; ok {
			return r.cells, r.del, true, nil
		}
	}
	// Newest part first: a pk may live in several parts (an update rewrites a
	// pk that already lives in an older part), and the newest one wins.
	for i := len(parts) - 1; i >= 0; i-- {
		p := parts[i]
		if !p.bloom.mayContain(key) {
			continue
		}
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
				return full[gi], dels[gi], true, nil
			}
			continue
		}
		if len(p.granules) > 0 {
			// v4 part but the key lies outside the part's PK span (a bloom
			// false positive): skip without decompressing.
			continue
		}
		pks, full, lerr := p.loadRows()
		if lerr != nil {
			return nil, false, false, lerr
		}
		dels, derr := p.loadDels()
		if derr != nil {
			return nil, false, false, derr
		}
		i := sort.Search(len(pks), func(i int) bool { return bytes.Compare(pks[i], key) >= 0 })
		if i < len(pks) && bytes.Equal(pks[i], key) {
			return full[i], dels[i], true, nil
		}
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
// whole part). Returns the cell (nil if absent or deleted).
func (t *mpartTx) lookupCol(table string, colIdx int, key []byte) ([]byte, error) {
	if ov := t.overlay[table]; ov != nil {
		if r, ok := ov.rows[string(key)]; ok {
			if r.del {
				return nil, nil
			}
			if colIdx >= 0 && colIdx < len(r.cells) {
				return r.cells[colIdx], nil
			}
			return nil, nil
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
			if colIdx >= 0 && colIdx < len(r.cells) {
				return r.cells[colIdx], nil
			}
			return nil, nil
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
				if gi < len(cells) {
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
			return cells[i], nil
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
		for k, r := range mp.rows {
			cur.rows[k] = r
		}
		cur.gen++
		s.lastWrite[tbl] = time.Now()
		if len(cur.rows) >= mpartFlushThreshold {
			if err := s.flushLocked(tbl); err != nil {
				return err
			}
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
		s := &mergeSrc{pks: pks, cells: cells, fulls: fulls, dels: dels, prio: p.seq}
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
		// Advance every other source whose head pk is the same: with the heap's
		// (pk, prio desc) order these are older versions shadowed by s.
		for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) {
			top := h.h[0]
			top.i += top.step
			if top.done() {
				heap.Pop(&h)
			} else {
				heap.Fix(&h, 0)
			}
		}
		var cell []byte
		var cells [][]byte
		if full {
			cells = s.fulls[i]
		} else if colIdx >= 0 {
			cell = s.cells[i]
		}
		if err := fn(pk, cell, cells, s.dels[i]); err != nil {
			return err
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
	rs := make([]*memRow, 0, len(mp.rows))
	for _, r := range mp.rows {
		rs = append(rs, r)
	}
	sort.Slice(rs, func(i, j int) bool { return bytes.Compare(rs[i].pk, rs[j].pk) < 0 })
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

// compactTable merges the table's mem + parts into fresh parts, dropping
// tombstoned rows and superseded versions, and reclaims the old part files.
// It returns the number of live rows after compaction.
func (s *mpartStore) compactTable(tbl string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &mpartTx{s: s, overlay: map[string]*memPart{}}
	entries, err := t.collectEntriesLocked(tbl, -1, true, nil, nil)
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
	// Rewrite in flush-sized chunks so compaction of a large table stays
	// bounded in memory.
	for _, p := range s.parts[tbl] {
		s.retirePart(p)
	}
	s.parts[tbl] = nil
	s.mem[tbl] = nil
	s.counts[tbl] = 0
	for start := 0; start < len(rows); start += mpartMergeChunk {
		end := start + mpartMergeChunk
		if end > len(rows) {
			end = len(rows)
		}
		p, err := s.writePart(tbl, rows[start:end])
		if err != nil {
			return 0, err
		}
		s.parts[tbl] = append(s.parts[tbl], p)
	}
	sortPartsBySeq(s.parts[tbl])
	s.counts[tbl] = int64(len(rows))
	return int64(len(rows)), nil
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

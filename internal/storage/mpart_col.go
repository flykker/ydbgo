package storage

import (
	"bytes"
	"container/heap"
	"math"
	"sort"
)

// Columnar read surface shared by the CSTORE (internal/kv column-major) and
// CSTORE2 (native mpart) engines. The sqlx.ColumnEngine implementation in
// cstore_col.go drives scans purely through this interface, so both backends
// are interchangeable behind their ENGINE= names.
type columnarTx interface {
	storeTx

	// colEachRange yields (pk, cell) pairs for one column inside
	// [plLower, plUpper) (nil = unbounded), in PK order, skipping deleted rows.
	// cell is only valid during the callback.
	colEachRange(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error
	// colEachRangeNoCopy is colEachRange; cells come from freshly decoded part
	// buffers so they are never reused, the "no copy" contract is automatic.
	colEachRangeNoCopy(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error
	// colDecodeNumeric materializes one numeric column as dense arrays, skipping
	// the per-cell reader allocation.
	colDecodeNumeric(table string, colIdx int, typ sqlType, plLower, plUpper []byte) (*numVec, error)
	// colRowKeysRange yields the live PKs inside the range in PK order.
	colRowKeysRange(table string, plLower, plUpper []byte, fn func(pk []byte) error) error
	// colRowKeysRangeDesc yields the live PKs inside the range in reverse PK
	// order.
	colRowKeysRangeDesc(table string, plLower, plUpper []byte, fn func(pk []byte) error) error
	// colRowCountRange counts live rows inside the range.
	colRowCountRange(table string, plLower, plUpper []byte) (int64, error)
	// countFor reports the table's live row count including this tx's deltas.
	countFor(table string) (int64, error)
	// colCell point-reads one column cell at an exact pk (nil if absent or
	// deleted). Used by columnar filter/predicate pushdown.
	colCell(table string, colIdx int, pk []byte) ([]byte, error)
}

// colCell resolves one cell through the merged view (overlay > mem > parts).
func (t *mpartTx) colCell(table string, colIdx int, pk []byte) ([]byte, error) {
	if colIdx < 0 {
		cells, del, found, err := t.lookup(table, pk)
		if err != nil || !found || del {
			return nil, err
		}
		if len(cells) > 0 {
			return cells[0], nil
		}
		return nil, nil
	}
	return t.lookupCol(table, colIdx, pk)
}

func (t *mpartTx) colEachRange(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error {
	return t.walkMerged(table, colIdx, false, false, plLower, plUpper, func(pk, cell []byte, _ [][]byte, del bool) error {
		if del {
			return nil
		}
		return fn(pk, cell)
	})
}

func (t *mpartTx) colEachRangeNoCopy(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error {
	return t.colEachRange(table, colIdx, plLower, plUpper, fn)
}

func (t *mpartTx) colRowKeysRange(table string, plLower, plUpper []byte, fn func(pk []byte) error) error {
	return t.walkMerged(table, -1, false, false, plLower, plUpper, func(pk []byte, _ []byte, _ [][]byte, del bool) error {
		if del {
			return nil
		}
		return fn(pk)
	})
}

func (t *mpartTx) colRowKeysRangeDesc(table string, plLower, plUpper []byte, fn func(pk []byte) error) error {
	return t.walkMerged(table, -1, false, true, plLower, plUpper, func(pk []byte, _ []byte, _ [][]byte, del bool) error {
		if del {
			return nil
		}
		return fn(pk)
	})
}

func (t *mpartTx) colRowCountRange(table string, plLower, plUpper []byte) (int64, error) {
	var n int64
	err := t.walkMerged(table, -1, false, false, plLower, plUpper, func(_ []byte, _ []byte, _ [][]byte, del bool) error {
		if !del {
			n++
		}
		return nil
	})
	return n, err
}

// colDecodeNumeric materializes one numeric column over the merged view. The
// semantics mirror the CSTORE implementation: cells that are not shaped as
// the expected numeric type fall back to a generic variant decode.
//
// When every part holds the column in the dense fixed-width layout, values are
// read straight from the pre-decoded int64 arrays (ClickHouse wide-part bulk
// decode): no per-cell varint parsing, no per-cell frame headers. Legacy parts
// (v1/v2 frames) and the mem/overlay sources fall back to per-cell decode.
func (t *mpartTx) colDecodeNumeric(table string, colIdx int, typ sqlType, plLower, plUpper []byte) (*numVec, error) {
	v := poolNumVec()
	v.typ = typ
	// Whole-window decode with no deletions/null/overlay: bulk-fill straight
	// from the cached dense part values (one append per part) instead of the
	// per-row callback.
	if plLower == nil && plUpper == nil && !t.cleared[table] && t.overlay[table] == nil {
		if fv, ok := t.denseNumericFast(table, colIdx, typ); ok {
			putNumVec(v)
			return fv, nil
		}
	}
	// Preallocate to the live row count so the append loop below never
	// reallocates: Go's ~1.25x growth for large slices would otherwise copy
	// ~8x the final size in total. countFor is a cheap cached read.
	if n, err := t.countFor(table); err == nil && n > 0 {
		if typ == tFloat {
			if cap(v.floats) < int(n) {
				v.floats = make([]float64, 0, int(n))
			}
		} else {
			if cap(v.ints) < int(n) {
				v.ints = make([]int64, 0, int(n))
			}
		}
	} else if typ == tFloat {
		if cap(v.floats) < 4096 {
			v.floats = make([]float64, 0, 4096)
		}
	} else {
		if cap(v.ints) < 4096 {
			v.ints = make([]int64, 0, 4096)
		}
	}
	err := t.walkMergedNumeric(table, colIdx, typ, plLower, plUpper, func(_ []byte, val int64, null bool, del bool) error {
		if del {
			return nil
		}
		if typ == tFloat {
			if null {
				v.floats = append(v.floats, 0)
				v.setNull(v.count)
			} else {
				v.floats = append(v.floats, math.Float64frombits(uint64(val)))
			}
		} else {
			if null {
				v.ints = append(v.ints, 0)
				v.setNull(v.count)
			} else {
				v.ints = append(v.ints, val)
			}
		}
		v.count++
		return nil
	})
	return v, err
}

// denseNumericFast bulk-fills one whole numeric column from dense part values
// only: every part's column dense, pairwise-disjoint PK windows, no
// tombstones, no NULL bits and no overlapping mem tail. Returns (vec, true)
// when it fully decoded the column. This is the SUM/GROUP hot path after an
// idle merge (single part): one bulk append per part instead of a per-row
// callback.
func (t *mpartTx) denseNumericFast(table string, colIdx int, typ sqlType) (*numVec, bool) {
	parts, mem := t.committedViewLocked(table)
	for _, p := range parts {
		if colIdx >= len(p.colFmts) {
			return nil, false
		}
		if _, dense := colFmtType(p.colFmts[colIdx]); !dense {
			return nil, false
		}
	}
	sort.Slice(parts, func(i, j int) bool { return bytes.Compare(parts[i].pkMin, parts[j].pkMin) < 0 })
	for k := 0; k < len(parts)-1; k++ {
		if bytes.Compare(parts[k].pkMax, parts[k+1].pkMin) >= 0 {
			return nil, false
		}
	}
	var memRows []*memRow
	if mem != nil && mapRows(mem) > 0 {
		mem.ensureCached()
		memRows = mem.cacheRows
		if len(memRows) > 0 && len(parts) > 0 && bytes.Compare(memRows[0].pk, parts[len(parts)-1].pkMax) <= 0 {
			return nil, false
		}
	}
	n := 0
	if c, err := t.countFor(table); err == nil && c > 0 {
		n = int(c)
	}
	v := poolNumVec()
	v.typ = typ
	if typ == tFloat {
		if cap(v.floats) < n {
			v.floats = make([]float64, 0, n)
		}
	} else {
		if cap(v.ints) < n {
			v.ints = make([]int64, 0, n)
		}
	}
	for _, p := range parts {
		vals, nulls, dense, err := p.loadColDense(colIdx)
		if err != nil || !dense {
			return nil, false
		}
		for _, w := range nulls {
			if w != 0 {
				return nil, false
			}
		}
		dels, err := p.loadDels()
		if err != nil {
			return nil, false
		}
		for _, d := range dels {
			if d {
				return nil, false
			}
		}
		if typ == tFloat {
			for _, x := range vals {
				v.floats = append(v.floats, math.Float64frombits(uint64(x)))
			}
		} else {
			v.ints = append(v.ints, vals...)
		}
		v.count += len(vals)
	}
	for _, r := range memRows {
		if r.del || colIdx >= len(r.cells) {
			return nil, false
		}
		val, null, ok := numericAtCell(r.cells[colIdx], typ)
		if !ok || null {
			return nil, false
		}
		if typ == tFloat {
			v.floats = append(v.floats, math.Float64frombits(uint64(val)))
		} else {
			v.ints = append(v.ints, val)
		}
		v.count++
	}
	return v, true
}

// walkMergedNumeric streams the table's numeric column values inside
// [plLower, plUpper) in PK order, resolving newest-writer-wins, invoking fn
// once per live-or-deleted row with the decoded int64 value (float columns
// pass the float64 bit pattern). Sources whose part column is dense feed
// pre-decoded arrays; everything else (legacy frames parts, mem, overlay)
// decodes per cell.
func (t *mpartTx) walkMergedNumeric(table string, colIdx int, typ sqlType, plLower, plUpper []byte, fn func(pk []byte, val int64, null, del bool) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range matches nothing
	}
	if t.snap {
		return t.walkMergedNumericLocked(table, colIdx, typ, plLower, plUpper, fn)
	}
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.walkMergedNumericLocked(table, colIdx, typ, plLower, plUpper, fn)
}

// fastNumericParts streams every part's numeric column without materializing
// PK lists, using part metadata for range/disjointness checks. Returns true
// when it fully handled the query. The fn callback receives a nil pk.
func (t *mpartTx) fastNumericParts(table string, parts []*mpart, mem *memPart, colIdx int, typ sqlType, fn func(pk []byte, val int64, null, del bool) error) bool {
	// Dense-only: legacy frames parts fall back to the cell path below.
	for _, p := range parts {
		if colIdx >= len(p.colFmts) {
			return false
		}
		if _, dense := colFmtType(p.colFmts[colIdx]); !dense {
			return false
		}
	}
	// Check pairwise-disjoint PK windows from metadata (pkMin/pkMax), so no
	// row is shadowed by a newer part and the parts can be concatenated.
	sort.Slice(parts, func(i, j int) bool { return bytes.Compare(parts[i].pkMin, parts[j].pkMin) < 0 })
	for k := 0; k < len(parts)-1; k++ {
		if bytes.Compare(parts[k].pkMax, parts[k+1].pkMin) >= 0 {
			return false
		}
	}
	// A mem tail is safe to append when its rows sit entirely above the last
	// part's PK window (the bench shape: trailing rows before an idle flush).
	var memRows []*memRow
	if mem != nil && mapRows(mem) > 0 {
		mem.ensureCached()
		memRows = mem.cacheRows
		if len(memRows) > 0 {
			if len(parts) > 0 {
				if bytes.Compare(memRows[0].pk, parts[len(parts)-1].pkMax) <= 0 {
					return false
				}
			}
		}
	}
	for _, p := range parts {
		vals, nulls, dense, err := p.loadColDense(colIdx)
		if err != nil || !dense {
			return false
		}
		dels, err := p.loadDels()
		if err != nil {
			return false
		}
		hasDel := false
		for _, d := range dels {
			if d {
				hasDel = true
				break
			}
		}
		if !hasDel {
			for i, val := range vals {
				null := nulls[i>>6]&(1<<(i&63)) != 0
				if err := fn(nil, val, null, false); err != nil {
					return false
				}
			}
			continue
		}
		for i, val := range vals {
			if dels[i] {
				continue
			}
			null := nulls[i>>6]&(1<<(i&63)) != 0
			if err := fn(nil, val, null, false); err != nil {
				return false
			}
		}
	}
	for _, r := range memRows {
		if r.del {
			continue
		}
		if colIdx < len(r.cells) {
			val, null, ok := numericAtCell(r.cells[colIdx], typ)
			if !ok {
				vv := makeReader(r.cells[colIdx]).Variant()
				if vv.null {
					null = true
				} else {
					val = vv.i
				}
			}
			if err := fn(nil, val, null, false); err != nil {
				return false
			}
		}
	}
	return true
}

func (t *mpartTx) walkMergedNumericLocked(table string, colIdx int, typ sqlType, plLower, plUpper []byte, fn func(pk []byte, val int64, null, del bool) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil
	}
	// Fast path: full-window aggregate over parts only (no mem, no overlay, no
	// tombstones), where the parts occupy pairwise-disjoint PK windows. This is
	// the bench shape after a flush — 15x65536 disjoint parts. The PK lists are
	// never materialized (no per-row []byte headers), disjointness comes from
	// the part metadata, and each part's dense values stream straight into the
	// aggregate. That is the ClickHouse "read column, ignore index" path.
	if plLower == nil && plUpper == nil && !t.cleared[table] && t.overlay[table] == nil {
		parts, mem := t.committedViewLocked(table)
		if t.fastNumericParts(table, parts, mem, colIdx, typ, fn) {
			return nil
		}
	}
	var h mergeHeap
	h.rev = false
	addPart := func(p *mpart, pks [][]byte, vals []int64, nulls []uint64, dels []bool) {
		lo, hi := sliceRange(pks, plLower, plUpper)
		if lo >= hi {
			return
		}
		s := &mergeSrc{pks: pks, vals: vals, nulls: nulls, dels: dels, prio: p.seq}
		s.i = lo
		s.last = hi
		s.step = 1
		h.h = append(h.h, s)
	}
	if !t.cleared[table] {
		parts, mem := t.committedViewLocked(table)
		for _, p := range parts {
			if !p.inRange(plLower, plUpper) {
				continue
			}
			pks, err := p.loadPks()
			if err != nil {
				return err
			}
			dels, err := p.loadDels()
			if err != nil {
				return err
			}
			vals, nulls, dense, err := p.loadColDense(colIdx)
			if err != nil {
				return err
			}
			if !dense {
				// Legacy frames part: decode cells into the dense shape once so
				// the merge loop below stays uniform.
				_, cells, err := p.loadCol(colIdx)
				if err != nil {
					return err
				}
				vals, nulls = decodeNumericCells(cells, typ)
			}
			addPart(p, pks, vals, nulls, dels)
		}
		if mem != nil {
			if err := addMemNumericSource(&h, mem, colIdx, typ, plLower, plUpper); err != nil {
				return err
			}
		}
	}
	if ov := t.overlay[table]; ov != nil {
		if err := addRowNumericSource(&h, ov.rows, colIdx, typ, prioOverlay, plLower, plUpper); err != nil {
			return err
		}
	}
	if len(h.h) == 0 {
		return nil
	}
	if len(h.h) == 1 {
		return walkSourceNumeric(h.h[0], typ, fn)
	}
	if sortDisjointSources(h.h, false) {
		for _, s := range h.h {
			if err := walkSourceNumeric(s, typ, fn); err != nil {
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
		for h.Len() > 0 && bytes.Equal(h.h[0].pks[h.h[0].i], pk) {
			top := h.h[0]
			top.i += top.step
			if top.done() {
				heap.Pop(&h)
			} else {
				heap.Fix(&h, 0)
			}
		}
		val, null, ok := numericAt(s, i, typ)
		if !ok {
			// Generic fallback: cell is not shaped as a numeric type.
			vv := makeReader(s.cells[i]).Variant()
			if vv.null {
				null = true
			} else {
				val = vv.i
			}
		}
		if err := fn(pk, val, null, s.dels[i]); err != nil {
			return err
		}
	}
	return nil
}

// numericAt reads the decoded int64 value of source s at position i. Dense
// sources read the pre-decoded array; legacy sources decode the cell. ok=false
// signals a cell not shaped as the expected numeric type.
func numericAt(s *mergeSrc, i int, typ sqlType) (val int64, null bool, ok bool) {
	if s.vals != nil {
		val = s.vals[i]
		null = s.nulls[i>>6]&(1<<(i&63)) != 0
		return val, null, true
	}
	var f float64
	val, f, null, ok = decodeNumericCell(s.cells[i], typ)
	if ok && typ == tFloat {
		val = int64(math.Float64bits(f))
	}
	return val, null, ok
}

// decodeNumericCells decodes a legacy frames cell slice into the dense
// [values][nulls] shape, falling back to the generic reader for cells not
// shaped as the expected numeric type.
func decodeNumericCells(cells [][]byte, typ sqlType) ([]int64, []uint64) {
	vals := make([]int64, len(cells))
	nulls := make([]uint64, (len(cells)+7)/8)
	for i, cell := range cells {
		val, null, ok := numericAtCell(cell, typ)
		if !ok {
			vv := makeReader(cell).Variant()
			if vv.null {
				null = true
			} else {
				val = vv.i
			}
		}
		vals[i] = val
		if null {
			nulls[i>>6] |= 1 << (i & 63)
		}
	}
	return vals, nulls
}

func numericAtCell(cell []byte, typ sqlType) (val int64, null bool, ok bool) {
	var f float64
	val, f, null, ok = decodeNumericCell(cell, typ)
	if ok && typ == tFloat {
		val = int64(math.Float64bits(f))
	}
	return val, null, ok
}

// walkSourceNumeric iterates one dense/legacy numeric source in pk order.
func walkSourceNumeric(s *mergeSrc, typ sqlType, fn func(pk []byte, val int64, null, del bool) error) error {
	for i := s.i; i != s.last; i += s.step {
		val, null, ok := numericAt(s, i, typ)
		if !ok {
			vv := makeReader(s.cells[i]).Variant()
			if vv.null {
				null = true
			} else {
				val = vv.i
			}
		}
		if err := fn(s.pks[i], val, null, s.dels[i]); err != nil {
			return err
		}
	}
	return nil
}

// addMemNumericSource builds a numeric source run from a mem part, decoding
// the column's cells once per query (the mem tail is small; parts carry the
// bulk).
func addMemNumericSource(h *mergeHeap, mp *memPart, colIdx int, typ sqlType, plLower, plUpper []byte) error {
	var rs []*memRow
	for _, r := range mp.rows {
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
	vals := make([]int64, len(rs))
	nulls := make([]uint64, (len(rs)+7)/8)
	for i, r := range rs {
		pks[i] = r.pk
		dels[i] = r.del
		if colIdx < len(r.cells) {
			val, null, ok := numericAtCell(r.cells[colIdx], typ)
			if !ok {
				vv := makeReader(r.cells[colIdx]).Variant()
				if vv.null {
					null = true
				} else {
					val = vv.i
				}
			}
			vals[i] = val
			if null {
				nulls[i>>6] |= 1 << (i & 63)
			}
		}
	}
	s := &mergeSrc{pks: pks, vals: vals, nulls: nulls, dels: dels, prio: prioMem}
	s.i = 0
	s.last = len(rs)
	s.step = 1
	h.h = append(h.h, s)
	return nil
}

// addRowNumericSource builds a numeric source run from a mem/overlay row map.
func addRowNumericSource(h *mergeHeap, rows map[string]*memRow, colIdx int, typ sqlType, prio int, plLower, plUpper []byte) error {
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
	vals := make([]int64, len(rs))
	nulls := make([]uint64, (len(rs)+7)/8)
	for i, r := range rs {
		pks[i] = r.pk
		dels[i] = r.del
		if colIdx < len(r.cells) {
			val, null, ok := numericAtCell(r.cells[colIdx], typ)
			if !ok {
				vv := makeReader(r.cells[colIdx]).Variant()
				if vv.null {
					null = true
				} else {
					val = vv.i
				}
			}
			vals[i] = val
			if null {
				nulls[i>>6] |= 1 << (i & 63)
			}
		}
	}
	s := &mergeSrc{pks: pks, vals: vals, nulls: nulls, dels: dels, prio: prio}
	s.i = 0
	s.last = len(rs)
	s.step = 1
	h.h = append(h.h, s)
	return nil
}

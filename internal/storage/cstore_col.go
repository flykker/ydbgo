package storage

import (
	"bytes"
	"errors"
	"fmt"
	"hash"
	"hash/maphash"
	"strings"

	sqlx "ydbgo/internal/sql"
)

var _ sqlx.ColumnEngine = (*Engine)(nil)

// pkBoundBytes encodes one side of a PK range into the encoded-pk bound used
// as the pk suffix of a scan range. The raw prefix encoding is the inclusive
// lower / exclusive upper bound; the exclusive lower / inclusive upper bound
// extends the prefix with a 0xff guard byte so it sits past every key sharing
// the prefix (type bytes are < 0xff).
func pkBoundBytes(b *sqlx.PKBound, isUpper bool) []byte {
	enc := []byte(EncodePK(b.Prefix))
	if isUpper == b.Incl {
		return append(enc, 0xff)
	}
	return enc
}

// PKRangeBytes converts a PK range into encoded scan bounds (nil = unbounded
// on that side). Used by the shard router to skip shards that cannot contain
// rows matching a WHERE-derived range.
func PKRangeBytes(r *sqlx.PKRange) (lower, upper []byte) {
	if r == nil {
		return nil, nil
	}
	if r.Lower != nil {
		lower = pkBoundBytes(r.Lower, false)
	}
	if r.Upper != nil {
		upper = pkBoundBytes(r.Upper, true)
	}
	return lower, upper
}

// ScanColumns implements sqlx.ColumnEngine for CSTORE tables: materializes
// only the requested columns by reading one contiguous range per column. Rows
// come back full-width in schema order with unrequested columns as Null.
func (e *Engine) ScanColumns(table string, colIdx []int, r *sqlx.PKRange) ([]sqlx.Row, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "CSTORE" {
		return nil, errors.New("table " + table + " is not a CSTORE table")
	}
	need := make([]bool, len(t.cols))
	for _, c := range colIdx {
		if c >= 0 && c < len(t.cols) {
			need[c] = true
		}
	}
	plLower, plUpper := PKRangeBytes(r)
	var rows []sqlx.Row
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		// Every column range iterates the same pks in the same sorted order
		// (rows are written/removed atomically across all cells), so values
		// are assembled by position with no per-cell key bookkeeping.
		type colVals struct{ vals []sqlValue }
		scans := make([]*colVals, len(t.cols))
		n := -1
		for i := range t.cols {
			if !need[i] {
				continue
			}
			cv := &colVals{}
			if err := ct.colEachRange(table, i, plLower, plUpper, func(_, cell []byte) error {
				cv.vals = append(cv.vals, makeReader(cell).Variant())
				return nil
			}); err != nil {
				return err
			}
			if n == -1 {
				n = len(cv.vals)
			} else if len(cv.vals) != n {
				return errors.New("column/pk count mismatch in " + table)
			}
			scans[i] = cv
		}
		if n < 0 {
			n = 0
		}
		for j := 0; j < n; j++ {
			row := make(sqlx.Row, len(t.cols))
			for i := range t.cols {
				row[i] = sqlx.NullValue
			}
			for i, cv := range scans {
				if cv != nil {
					row[i] = toSQLValue(cv.vals[j])
				}
			}
			rows = append(rows, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ColumnCount implements sqlx.ColumnEngine: counts row markers without
// reading any cell data.
func (e *Engine) ColumnCount(table string, r *sqlx.PKRange) (int64, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return 0, err
	}
	if t.engine != "CSTORE" {
		return 0, errors.New("table " + table + " is not a CSTORE table")
	}
	if r == nil {
		// Whole-table COUNT: read the live-row counter in O(1) instead of
		// scanning row markers.
		var n int64
		err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
			ct, ok := tx.(*cstoreTx)
			if !ok {
				return errors.New("table " + table + " is not backed by the CSTORE store")
			}
			n, err = ct.countFor(table)
			return err
		})
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	plLower, plUpper := PKRangeBytes(r)
	var n int64
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		n, err = ct.colRowCountRange(table, plLower, plUpper)
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ColumnAggregate implements sqlx.ColumnEngine: computes a whole-table
// aggregate over one column by scanning that column's contiguous range.
func (e *Engine) ColumnAggregate(table string, colIdx int, agg string, r *sqlx.PKRange) (sqlx.Value, error) {
	vals, err := e.ColumnAggregates(table, colIdx, []string{agg}, r)
	if err != nil {
		return sqlx.NullValue, err
	}
	return vals[0], nil
}

// ColumnAggregates implements sqlx.ColumnEngine: computes several whole-table
// aggregates over one column in a single scan of that column's range.
func (e *Engine) ColumnAggregates(table string, colIdx int, aggs []string, r *sqlx.PKRange) ([]sqlx.Value, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "CSTORE" {
		return nil, errors.New("table " + table + " is not a CSTORE table")
	}
	if colIdx < 0 || colIdx >= len(t.cols) {
		return nil, fmt.Errorf("column index %d out of range", colIdx)
	}
	var flags uint8
	for _, a := range aggs {
		switch strings.ToLower(a) {
		case "count":
			flags |= accCount
		case "sum":
			flags |= accSum
		case "min":
			flags |= accMin
		case "max":
			flags |= accMax
		case "avg":
			flags |= accAvg
		}
	}
	plLower, plUpper := PKRangeBytes(r)
	var (
		cnt            int64
		sum            float64
		sumInt         = true
		any            bool
		minV, maxV     sqlValue
		minSet, maxSet bool
		avgTotal       float64
		avgN           int64
	)
	ct := t.cols[colIdx].typ
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct2, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		// Numeric columns: bulk-decode the range into dense arrays, then fold
		// the aggregates over them in a tight loop (vectorized scan). String
		// columns fall back to the generic per-cell reader path.
		if ct == tInt || ct == tFloat || ct == tTimestamp {
			v, err := ct2.colDecodeNumeric(table, colIdx, ct, plLower, plUpper)
			if err != nil {
				return err
			}
			return aggNumVec(v, flags, &cnt, &sum, &sumInt, &any, &minV, &maxV, &minSet, &maxSet, &avgTotal, &avgN)
		}
		return ct2.colEachRange(table, colIdx, plLower, plUpper, func(_, cell []byte) error {
			v := makeReader(cell).Variant()
			return aggAddValue(flags, v, &cnt, &sum, &sumInt, &any, &minV, &maxV, &minSet, &maxSet, &avgTotal, &avgN)
		})
	})
	if err != nil {
		return nil, err
	}
	return aggResults(aggs, flags, cnt, sum, sumInt, any, minV, maxV, minSet, maxSet, avgTotal, avgN), nil
}

// aggNumVec folds a bulk-decoded numeric column (numVec) into whole-column
// aggregate accumulators. This is the vectorized inner loop over the dense
// arrays; cell values are already decoded, so only the requested aggregate
// branches are evaluated. Semantic parity with aggAddValue.
func aggNumVec(v *numVec, flags uint8, cnt *int64, sum *float64, sumInt *bool, any *bool, minV, maxV *sqlValue, minSet, maxSet *bool, avgTotal *float64, avgN *int64) error {
	if v.typ == tFloat {
		for p := 0; p < v.count; p++ {
			if v.nullAt(p) {
				continue
			}
			f := v.floats[p]
			if flags&accCount != 0 {
				*cnt++
			}
			if flags&accSum != 0 {
				*sumInt = false
				*sum += f
				*any = true
			}
			if flags&(accMin|accMax) != 0 {
				val := sqlValue{typ: tFloat, f: f}
				if !*minSet {
					*minV, *maxV = val, val
					*minSet, *maxSet = true, true
				} else {
					if flags&accMin != 0 {
						if c, err := compareSQLValue(val, *minV); err != nil {
							return err
						} else if c < 0 {
							*minV = val
						}
					}
					if flags&accMax != 0 {
						if c, err := compareSQLValue(val, *maxV); err != nil {
							return err
						} else if c > 0 {
							*maxV = val
						}
					}
				}
			}
			if flags&accAvg != 0 {
				*avgTotal += f
				*avgN++
			}
		}
		return nil
	}
	for p := 0; p < v.count; p++ {
		if v.nullAt(p) {
			continue
		}
		i := v.ints[p]
		if flags&accCount != 0 {
			*cnt++
		}
		if flags&accSum != 0 {
			*sum += float64(i)
			*any = true
		}
		if flags&(accMin|accMax) != 0 {
			val := sqlValue{typ: v.typ, i: i}
			if !*minSet {
				*minV, *maxV = val, val
				*minSet, *maxSet = true, true
			} else {
				if flags&accMin != 0 {
					if c, err := compareSQLValue(val, *minV); err != nil {
						return err
					} else if c < 0 {
						*minV = val
					}
				}
				if flags&accMax != 0 {
					if c, err := compareSQLValue(val, *maxV); err != nil {
						return err
					} else if c > 0 {
						*maxV = val
					}
				}
			}
		}
		if flags&accAvg != 0 {
			*avgTotal += float64(i)
			*avgN++
		}
	}
	return nil
}

// aggAddValue folds one decoded cell value into whole-column aggregate
// accumulators according to flags (accCount|accSum|accMin|accMax|accAvg).
func aggAddValue(flags uint8, v sqlValue, cnt *int64, sum *float64, sumInt *bool, any *bool, minV, maxV *sqlValue, minSet, maxSet *bool, avgTotal *float64, avgN *int64) error {
	if v.null {
		return nil
	}
	if flags&accCount != 0 {
		*cnt++
	}
	if flags&accSum != 0 {
		if v.typ != tInt {
			*sumInt = false
		}
		switch v.typ {
		case tInt:
			*sum += float64(v.i)
		case tFloat:
			*sum += v.f
		default:
			return fmt.Errorf("sum requires numeric")
		}
		*any = true
	}
	if flags&(accMin|accMax) != 0 {
		if !*minSet {
			*minV, *maxV = v, v
			*minSet, *maxSet = true, true
		} else {
			if flags&accMin != 0 {
				if c, err := compareSQLValue(v, *minV); err != nil {
					return err
				} else if c < 0 {
					*minV = v
				}
			}
			if flags&accMax != 0 {
				if c, err := compareSQLValue(v, *maxV); err != nil {
					return err
				} else if c > 0 {
					*maxV = v
				}
			}
		}
	}
	if flags&accAvg != 0 {
		switch v.typ {
		case tInt:
			*avgTotal += float64(v.i)
		case tFloat:
			*avgTotal += v.f
		default:
			return fmt.Errorf("avg requires numeric")
		}
		*avgN++
	}
	return nil
}

// aggResults renders whole-column aggregate accumulators into output values.
func aggResults(aggs []string, flags uint8, cnt int64, sum float64, sumInt, any bool, minV, maxV sqlValue, minSet, maxSet bool, avgTotal float64, avgN int64) []sqlx.Value {
	out := make([]sqlx.Value, len(aggs))
	for i, a := range aggs {
		switch strings.ToLower(a) {
		case "count":
			out[i] = sqlx.IntValue(cnt)
		case "sum":
			if !any {
				out[i] = sqlx.NullValue
			} else if sumInt {
				out[i] = sqlx.IntValue(int64(sum))
			} else {
				out[i] = sqlx.FloatValue(sum)
			}
		case "min":
			if !minSet {
				out[i] = sqlx.NullValue
			} else {
				out[i] = toSQLValue(minV)
			}
		case "max":
			if !maxSet {
				out[i] = sqlx.NullValue
			} else {
				out[i] = toSQLValue(maxV)
			}
		case "avg":
			if avgN == 0 {
				out[i] = sqlx.NullValue
			} else {
				out[i] = sqlx.FloatValue(avgTotal / float64(avgN))
			}
		default:
			out[i] = sqlx.NullValue
		}
	}
	return out
}

// matchesFilter reports whether a decoded cell satisfies a ColumnFilter. The
// predicate literal is compared as a value of the column's type.
func matchesFilter(v sqlValue, pred *sqlx.ColumnFilter) (bool, error) {
	if v.null {
		return false, nil
	}
	if pred.Op == "LIKE" {
		if v.typ != tString {
			return false, nil
		}
		return strings.HasPrefix(v.s, pred.Lit.Str), nil
	}
	lit := fromSQLValue(pred.Lit)
	c, err := compareSQLValue(v, lit)
	if err != nil {
		return false, err
	}
	return c == 0, nil
}

// ColumnCountFiltered implements sqlx.ColumnEngine: counts rows whose
// predicate column cell matches pred, by scanning that column only.
func (e *Engine) ColumnCountFiltered(table string, pred *sqlx.ColumnFilter, r *sqlx.PKRange) (int64, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return 0, err
	}
	if t.engine != "CSTORE" {
		return 0, errors.New("table " + table + " is not a CSTORE table")
	}
	if pred.Col < 0 || pred.Col >= len(t.cols) {
		return 0, fmt.Errorf("column index %d out of range", pred.Col)
	}
	plLower, plUpper := PKRangeBytes(r)
	var n int64
	if matched, err := e.indexMatchRange(t, pred, plLower, plUpper, func(pk []byte) error {
		n++
		return nil
	}); err != nil {
		return 0, err
	} else if matched {
		return n, nil
	}
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		ctyp := t.cols[pred.Col].typ
		// Numeric predicate columns use the vectorized decode: the column
		// range is bulk-decoded into a dense array, then equality-matched
		// against the literal in a tight loop.
		if ctyp == tInt || ctyp == tFloat || ctyp == tTimestamp {
			v, err := ct.colDecodeNumeric(table, pred.Col, ctyp, plLower, plUpper)
			if err != nil {
				return err
			}
			if v.typ == tFloat {
				for p := 0; p < v.count; p++ {
					if v.nullAt(p) {
						continue
					}
					val := sqlValue{typ: tFloat, f: v.floats[p]}
					m, err := matchesFilter(val, pred)
					if err != nil {
						return err
					}
					if m {
						n++
					}
				}
				return nil
			}
			for p := 0; p < v.count; p++ {
				if v.nullAt(p) {
					continue
				}
				val := sqlValue{typ: ctyp, i: v.ints[p]}
				m, err := matchesFilter(val, pred)
				if err != nil {
					return err
				}
				if m {
					n++
				}
			}
			return nil
		}
		return ct.colEachRangeNoCopy(table, pred.Col, plLower, plUpper, func(_, cell []byte) error {
			m, err := matchesFilter(makeReader(cell).Variant(), pred)
			if err != nil {
				return err
			}
			if m {
				n++
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ColumnAggregatesFiltered implements sqlx.ColumnEngine: computes whole-table
// aggregates over one column, restricted to rows whose predicate column cell
// matches pred. When the aggregate and predicate are the same column a single
// scan suffices; otherwise the predicate column is scanned first to build a
// keep mask and the aggregate column is accumulated at matching positions (both
// scans iterate the same pks in the same sorted order).
func (e *Engine) ColumnAggregatesFiltered(table string, colIdx int, aggs []string, pred *sqlx.ColumnFilter, r *sqlx.PKRange) ([]sqlx.Value, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "CSTORE" {
		return nil, errors.New("table " + table + " is not a CSTORE table")
	}
	if colIdx < 0 || colIdx >= len(t.cols) {
		return nil, fmt.Errorf("column index %d out of range", colIdx)
	}
	plLower, plUpper := PKRangeBytes(r)
	acc := newColAccum(aggs)
	var pks [][]byte
	if matched, err := e.indexMatchRange(t, pred, plLower, plUpper, func(pk []byte) error {
		pks = append(pks, append([]byte(nil), pk...))
		return nil
	}); err != nil {
		return nil, err
	} else if matched {
		// Point-read the aggregate column at each matching pk.
		err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
			ct, ok := tx.(*cstoreTx)
			if !ok {
				return errors.New("table " + table + " is not backed by the CSTORE store")
			}
			for _, pk := range pks {
				cell, err := ct.get(cstoreColKey(table, colIdx, pk))
				if err != nil {
					return err
				}
				if len(cell) == 0 {
					continue
				}
				if err := acc.add(makeReader(cell).Variant()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return acc.result(aggs), nil
	}
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		if colIdx == pred.Col {
			ctyp := t.cols[colIdx].typ
			// Same-column numeric filter: vectorized decode, then equality
			// match + accumulate in one tight pass.
			if ctyp == tInt || ctyp == tFloat || ctyp == tTimestamp {
				v, err := ct.colDecodeNumeric(table, colIdx, ctyp, plLower, plUpper)
				if err != nil {
					return err
				}
				if v.typ == tFloat {
					for p := 0; p < v.count; p++ {
						if v.nullAt(p) {
							continue
						}
						val := sqlValue{typ: tFloat, f: v.floats[p]}
						m, err := matchesFilter(val, pred)
						if err != nil {
							return err
						}
						if m {
							acc.addNum(tFloat, 0, v.floats[p])
						}
					}
					return nil
				}
				for p := 0; p < v.count; p++ {
					if v.nullAt(p) {
						continue
					}
					val := sqlValue{typ: ctyp, i: v.ints[p]}
					m, err := matchesFilter(val, pred)
					if err != nil {
						return err
					}
					if m {
						acc.addNum(ctyp, v.ints[p], 0)
					}
				}
				return nil
			}
			return ct.colEachRangeNoCopy(table, colIdx, plLower, plUpper, func(_, cell []byte) error {
				v := makeReader(cell).Variant()
				m, err := matchesFilter(v, pred)
				if err != nil {
					return err
				}
				if !m {
					return nil
				}
				return acc.add(v)
			})
		}
		var keep []bool
		if err := ct.colEachRangeNoCopy(table, pred.Col, plLower, plUpper, func(_, cell []byte) error {
			m, err := matchesFilter(makeReader(cell).Variant(), pred)
			if err != nil {
				return err
			}
			keep = append(keep, m)
			return nil
		}); err != nil {
			return err
		}
		j := 0
		return ct.colEachRangeNoCopy(table, colIdx, plLower, plUpper, func(_, cell []byte) error {
			var m bool
			if j < len(keep) {
				m = keep[j]
			}
			j++
			if !m {
				return nil
			}
			return acc.add(makeReader(cell).Variant())
		})
	})
	if err != nil {
		return nil, err
	}
	return acc.result(aggs), nil
}

// ScanColumnsFiltered implements sqlx.ColumnEngine: returns every row whose
// predicate column cell matches pred, with only the given columns materialized
// (unrequested columns come back Null). The predicate column is scanned first
// to build a keep mask; each requested column then yields only its matching
// positions (both scans iterate the same pks in the same sorted order).
func (e *Engine) ScanColumnsFiltered(table string, colIdx []int, pred *sqlx.ColumnFilter, r *sqlx.PKRange) ([]sqlx.Row, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "CSTORE" {
		return nil, errors.New("table " + table + " is not a CSTORE table")
	}
	need := make([]bool, len(t.cols))
	for _, c := range colIdx {
		if c >= 0 && c < len(t.cols) {
			need[c] = true
		}
	}
	plLower, plUpper := PKRangeBytes(r)
	var rows []sqlx.Row
	var pks [][]byte
	if matched, err := e.indexMatchRange(t, pred, plLower, plUpper, func(pk []byte) error {
		pks = append(pks, append([]byte(nil), pk...))
		return nil
	}); err != nil {
		return nil, err
	} else if matched {
		// Point-read the requested columns at each matching pk.
		err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
			ct, ok := tx.(*cstoreTx)
			if !ok {
				return errors.New("table " + table + " is not backed by the CSTORE store")
			}
			for _, pk := range pks {
				row := make(sqlx.Row, len(t.cols))
				for i := range t.cols {
					row[i] = sqlx.NullValue
				}
				for i := range t.cols {
					if !need[i] {
						continue
					}
					cell, err := ct.get(cstoreColKey(table, i, pk))
					if err != nil {
						return err
					}
					if len(cell) > 0 {
						row[i] = toSQLValue(makeReader(cell).Variant())
					}
				}
				rows = append(rows, row)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		var keep []bool
		if err := ct.colEachRange(table, pred.Col, plLower, plUpper, func(_, cell []byte) error {
			m, err := matchesFilter(makeReader(cell).Variant(), pred)
			if err != nil {
				return err
			}
			keep = append(keep, m)
			return nil
		}); err != nil {
			return err
		}
		var matchN int
		for _, m := range keep {
			if m {
				matchN++
			}
		}
		type colVals struct{ vals []sqlValue }
		scans := make([]*colVals, len(t.cols))
		for i := range t.cols {
			if !need[i] {
				continue
			}
			cv := &colVals{}
			j := 0
			if err := ct.colEachRange(table, i, plLower, plUpper, func(_, cell []byte) error {
				var m bool
				if j < len(keep) {
					m = keep[j]
				}
				j++
				if !m {
					return nil
				}
				cv.vals = append(cv.vals, makeReader(cell).Variant())
				return nil
			}); err != nil {
				return err
			}
			scans[i] = cv
		}
		for j := 0; j < matchN; j++ {
			row := make(sqlx.Row, len(t.cols))
			for i := range t.cols {
				row[i] = sqlx.NullValue
			}
			for i, cv := range scans {
				if cv != nil && j < len(cv.vals) {
					row[i] = toSQLValue(cv.vals[j])
				}
			}
			rows = append(rows, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// colAccum accumulates one or more aggregate functions over a stream of cells.
type colAccum struct {
	flags          uint8
	cnt            int64
	sum            float64
	sumInt         bool
	any            bool
	minV, maxV     sqlValue
	minSet, maxSet bool
	avgTotal       float64
	avgN           int64
}

const (
	accCount uint8 = 1 << iota
	accSum
	accMin
	accMax
	accAvg
)

func newColAccum(aggs []string) *colAccum {
	var f uint8
	for _, a := range aggs {
		switch strings.ToLower(a) {
		case "count":
			f |= accCount
		case "sum":
			f |= accSum
		case "min":
			f |= accMin
		case "max":
			f |= accMax
		case "avg":
			f |= accAvg
		}
	}
	return &colAccum{flags: f, sumInt: true}
}

// star counts one row (COUNT(*) over the group).
func (a *colAccum) star() {
	if a.flags&accCount != 0 {
		a.cnt++
	}
}

// addNum folds one non-null numeric cell (already decoded by the vectorized
// scan) into the accumulators, skipping the generic sqlValue/reader path.
func (a *colAccum) addNum(typ sqlType, i int64, f float64) {
	if a.flags&accCount != 0 {
		a.cnt++
	}
	if a.flags&accSum != 0 {
		if typ != tInt {
			a.sumInt = false
		}
		if typ == tFloat {
			a.sum += f
		} else {
			a.sum += float64(i)
		}
		a.any = true
	}
	if a.flags&(accMin|accMax) != 0 {
		v := sqlValue{typ: typ, i: i, f: f}
		if !a.minSet {
			a.minV, a.maxV = v, v
			a.minSet, a.maxSet = true, true
		} else {
			if a.flags&accMin != 0 {
				if c, err := compareSQLValue(v, a.minV); err == nil && c < 0 {
					a.minV = v
				}
			}
			if a.flags&accMax != 0 {
				if c, err := compareSQLValue(v, a.maxV); err == nil && c > 0 {
					a.maxV = v
				}
			}
		}
	}
	if a.flags&accAvg != 0 {
		if typ == tFloat {
			a.avgTotal += f
		} else {
			a.avgTotal += float64(i)
		}
		a.avgN++
	}
}

// add folds one non-null cell value into the accumulators.
func (a *colAccum) add(v sqlValue) error {
	if v.null {
		return nil
	}
	if a.flags&accCount != 0 {
		a.cnt++
	}
	if a.flags&accSum != 0 {
		if v.typ != tInt {
			a.sumInt = false
		}
		switch v.typ {
		case tInt:
			a.sum += float64(v.i)
		case tFloat:
			a.sum += v.f
		default:
			return fmt.Errorf("sum requires numeric")
		}
		a.any = true
	}
	if a.flags&(accMin|accMax) != 0 {
		if !a.minSet {
			a.minV, a.maxV = v, v
			a.minSet, a.maxSet = true, true
		} else {
			if a.flags&accMin != 0 {
				if c, err := compareSQLValue(v, a.minV); err != nil {
					return err
				} else if c < 0 {
					a.minV = v
				}
			}
			if a.flags&accMax != 0 {
				if c, err := compareSQLValue(v, a.maxV); err != nil {
					return err
				} else if c > 0 {
					a.maxV = v
				}
			}
		}
	}
	if a.flags&accAvg != 0 {
		switch v.typ {
		case tInt:
			a.avgTotal += float64(v.i)
		case tFloat:
			a.avgTotal += v.f
		default:
			return fmt.Errorf("avg requires numeric")
		}
		a.avgN++
	}
	return nil
}

func (a *colAccum) result(aggs []string) []sqlx.Value {
	out := make([]sqlx.Value, len(aggs))
	for i, agg := range aggs {
		switch strings.ToLower(agg) {
		case "count":
			out[i] = sqlx.IntValue(a.cnt)
		case "sum":
			if !a.any {
				out[i] = sqlx.NullValue
			} else if a.sumInt {
				out[i] = sqlx.IntValue(int64(a.sum))
			} else {
				out[i] = sqlx.FloatValue(a.sum)
			}
		case "min":
			if a.minSet {
				out[i] = toSQLValue(a.minV)
			} else {
				out[i] = sqlx.NullValue
			}
		case "max":
			if a.maxSet {
				out[i] = toSQLValue(a.maxV)
			} else {
				out[i] = sqlx.NullValue
			}
		case "avg":
			if a.avgN == 0 {
				out[i] = sqlx.NullValue
			} else {
				out[i] = sqlx.FloatValue(a.avgTotal / float64(a.avgN))
			}
		default:
			out[i] = sqlx.NullValue
		}
	}
	return out
}

// ColumnGroupedAggregates implements sqlx.ColumnEngine: computes aggregates
// per group of one column over a single pass. Each output row is [group value,
// the aggregate results of gas in order]. Columns referenced by several gas
// entries are scanned once. The group key is the raw cell bytes hashed in
// place (zero per-row allocation); the group value is decoded only when a new
// distinct group appears.
func (e *Engine) ColumnGroupedAggregates(table string, groupCol int, gas []sqlx.GroupAgg, r *sqlx.PKRange) ([]sqlx.Row, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "CSTORE" {
		return nil, errors.New("table " + table + " is not a CSTORE table")
	}
	if groupCol < 0 || groupCol >= len(t.cols) {
		return nil, fmt.Errorf("group column index %d out of range", groupCol)
	}
	plLower, plUpper := PKRangeBytes(r)
	var rows []sqlx.Row
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		type grp struct {
			raw  []byte // raw cell bytes of the group value (collision check)
			gval sqlx.Value
			accs []*colAccum
		}
		idx := map[uint64]*grp{}
		var order []*grp
		var hh hash.Hash64 = new(maphash.Hash)
		// Group gas entries by their referenced column so each column is
		// scanned exactly once (e.g. SUM(lat)+COUNT(lat) share one scan).
		type colAgg struct {
			col int
			k   []int // gas indices
		}
		colSeen := map[int]bool{}
		var colAggs []colAgg
		var starK []int
		for k, ga := range gas {
			if ga.Col < 0 {
				starK = append(starK, k)
				continue
			}
			if !colSeen[ga.Col] {
				colSeen[ga.Col] = true
				colAggs = append(colAggs, colAgg{col: ga.Col})
			}
			ca := &colAggs[len(colAggs)-1]
			for i := range colAggs {
				if colAggs[i].col == ga.Col {
					ca = &colAggs[i]
					break
				}
			}
			ca.k = append(ca.k, k)
		}
		// Pass 1: scan the group column. Each row's group is resolved once and
		// remembered; aggregate columns then accumulate directly in row order.
		var groups []*grp
		gi := groupCol
		if err := ct.colEachRangeNoCopy(table, gi, plLower, plUpper, func(_, cell []byte) error {
			hh.Reset()
			hh.Write(cell)
			h := hh.Sum64()
			g := idx[h]
			if g != nil && !bytes.Equal(g.raw, cell) {
				// hash collision: fall back to a linear scan over the few
				// distinct groups so results stay exact.
				g = nil
				for _, cand := range order {
					if bytes.Equal(cand.raw, cell) {
						g = cand
						break
					}
				}
			}
			if g == nil {
				g = &grp{raw: append([]byte(nil), cell...), gval: toSQLValue(makeReader(cell).Variant()), accs: make([]*colAccum, len(gas))}
				for k, ga := range gas {
					g.accs[k] = newColAccum(ga.Aggs)
				}
				idx[h] = g
				order = append(order, g)
			}
			groups = append(groups, g)
			return nil
		}); err != nil {
			return err
		}
		for _, k := range starK {
			// COUNT(*) counts every row of the group.
			for _, g := range groups {
				g.accs[k].star()
			}
		}
		for _, ca := range colAggs {
			ctyp := t.cols[ca.col].typ
			// Numeric aggregate columns use the vectorized decode: the column
			// range is bulk-decoded into a dense array, then folded into each
			// position's group accumulators (no per-cell reader).
			if ctyp == tInt || ctyp == tFloat || ctyp == tTimestamp {
				v, err := ct.colDecodeNumeric(table, ca.col, ctyp, plLower, plUpper)
				if err != nil {
					return err
				}
				if v.count != len(groups) {
					return errors.New("column/pk count mismatch in " + table)
				}
				if v.typ == tFloat {
					for p := 0; p < v.count; p++ {
						if v.nullAt(p) {
							continue
						}
						g := groups[p]
						for _, k := range ca.k {
							g.accs[k].addNum(tFloat, 0, v.floats[p])
						}
					}
				} else {
					for p := 0; p < v.count; p++ {
						if v.nullAt(p) {
							continue
						}
						g := groups[p]
						for _, k := range ca.k {
							g.accs[k].addNum(v.typ, v.ints[p], 0)
						}
					}
				}
				continue
			}
			rest := groups
			if err := ct.colEachRangeNoCopy(table, ca.col, plLower, plUpper, func(_, cell []byte) error {
				if len(rest) == 0 {
					return errors.New("column/pk count mismatch in " + table)
				}
				v := makeReader(cell).Variant()
				for _, k := range ca.k {
					if err := rest[0].accs[k].add(v); err != nil {
						return err
					}
				}
				rest = rest[1:]
				return nil
			}); err != nil {
				return err
			}
		}
		for _, g := range order {
			row := make(sqlx.Row, 1+len(gas))
			row[0] = g.gval
			for k, ga := range gas {
				row[1+k] = g.accs[k].result(ga.Aggs)[0]
			}
			rows = append(rows, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ScanTopN implements sqlx.ColumnEngine for CSTORE tables: returns up to limit
// rows with only colIdx materialized, ordered by primary key (descending when
// desc is true). It walks the PK index in index order and stops after limit
// live rows, then point-reads the requested columns, so the cost is O(limit)
// instead of a full scan + sort.
func (e *Engine) ScanTopN(table string, colIdx []int, r *sqlx.PKRange, desc bool, limit int) ([]sqlx.Row, error) {
	unlock := e.readLock()
	defer unlock()
	t, err := e.getTable(table)
	if err != nil {
		return nil, err
	}
	if t.engine != "CSTORE" {
		return nil, errors.New("table " + table + " is not a CSTORE table")
	}
	if limit < 0 {
		limit = 0
	}
	need := make([]bool, len(t.cols))
	for _, c := range colIdx {
		if c >= 0 && c < len(t.cols) {
			need[c] = true
		}
	}
	plLower, plUpper := PKRangeBytes(r)
	var pks [][]byte
	var rows []sqlx.Row
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		walk := func(pk []byte) error {
			if len(pks) >= limit {
				return errStop
			}
			pks = append(pks, append([]byte(nil), pk...))
			return nil
		}
		var scanErr error
		if desc {
			scanErr = ct.colRowKeysRangeDesc(table, plLower, plUpper, walk)
		} else {
			scanErr = ct.colRowKeysRange(table, plLower, plUpper, walk)
		}
		if scanErr != nil && scanErr != errStop {
			return scanErr
		}
		for _, pk := range pks {
			row := make(sqlx.Row, len(t.cols))
			for i := range t.cols {
				row[i] = sqlx.NullValue
			}
			for i := range t.cols {
				if !need[i] {
					continue
				}
				cell, err := ct.get(cstoreColKey(table, i, pk))
				if err != nil {
					return err
				}
				if cell != nil {
					row[i] = toSQLValue(makeReader(cell).Variant())
				}
			}
			rows = append(rows, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// compareSQLValue compares two non-null sqlValues.
func compareSQLValue(a, b sqlValue) (int, error) {
	if a.typ != b.typ {
		return 0, fmt.Errorf("cannot compare %s and %s", a.typ, b.typ)
	}
	switch a.typ {
	case tInt:
		switch {
		case a.i < b.i:
			return -1, nil
		case a.i > b.i:
			return 1, nil
		}
		return 0, nil
	case tFloat:
		switch {
		case a.f < b.f:
			return -1, nil
		case a.f > b.f:
			return 1, nil
		}
		return 0, nil
	case tString:
		return strings.Compare(a.s, b.s), nil
	case tBool:
		if a.b == b.b {
			return 0, nil
		}
		if a.b {
			return 1, nil
		}
		return -1, nil
	case tTimestamp:
		switch {
		case a.i < b.i:
			return -1, nil
		case a.i > b.i:
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("cannot compare type %s", a.typ)
}

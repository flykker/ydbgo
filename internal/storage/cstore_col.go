package storage

import (
	"errors"
	"fmt"
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
	plLower, plUpper := PKRangeBytes(r)
	var n int64
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		return ct.colRowKeysRange(table, plLower, plUpper, func(pk []byte) error {
			n++
			return nil
		})
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
	want := map[string]bool{}
	for _, a := range aggs {
		want[strings.ToLower(a)] = true
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
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
		return ct.colEachRange(table, colIdx, plLower, plUpper, func(_, cell []byte) error {
			v := makeReader(cell).Variant()
			if v.null {
				return nil
			}
			if want["count"] {
				cnt++
			}
			if want["sum"] {
				if v.typ != tInt {
					sumInt = false
				}
				switch v.typ {
				case tInt:
					sum += float64(v.i)
				case tFloat:
					sum += v.f
				default:
					return fmt.Errorf("sum requires numeric")
				}
				any = true
			}
			if want["min"] || want["max"] {
				if !minSet {
					minV, maxV = v, v
					minSet, maxSet = true, true
				} else {
					if want["min"] {
						if c, err := compareSQLValue(v, minV); err != nil {
							return err
						} else if c < 0 {
							minV = v
						}
					}
					if want["max"] {
						if c, err := compareSQLValue(v, maxV); err != nil {
							return err
						} else if c > 0 {
							maxV = v
						}
					}
				}
			}
			if want["avg"] {
				switch v.typ {
				case tInt:
					avgTotal += float64(v.i)
				case tFloat:
					avgTotal += v.f
				default:
					return fmt.Errorf("avg requires numeric")
				}
				avgN++
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
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
	return out, nil
}

// colAccum accumulates one or more aggregate functions over a stream of cells.
type colAccum struct {
	want          map[string]bool
	cnt           int64
	sum           float64
	sumInt        bool
	any           bool
	minV, maxV    sqlValue
	minSet, maxSet bool
	avgTotal      float64
	avgN          int64
}

func newColAccum(aggs []string) *colAccum {
	w := map[string]bool{}
	for _, a := range aggs {
		w[strings.ToLower(a)] = true
	}
	return &colAccum{want: w, sumInt: true}
}

// star counts one row (COUNT(*) over the group).
func (a *colAccum) star() {
	if a.want["count"] {
		a.cnt++
	}
}

// add folds one non-null cell value into the accumulators.
func (a *colAccum) add(v sqlValue) error {
	if v.null {
		return nil
	}
	if a.want["count"] {
		a.cnt++
	}
	if a.want["sum"] {
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
	if a.want["min"] || a.want["max"] {
		if !a.minSet {
			a.minV, a.maxV = v, v
			a.minSet, a.maxSet = true, true
		} else {
			if a.want["min"] {
				if c, err := compareSQLValue(v, a.minV); err != nil {
					return err
				} else if c < 0 {
					a.minV = v
				}
			}
			if a.want["max"] {
				if c, err := compareSQLValue(v, a.maxV); err != nil {
					return err
				} else if c > 0 {
					a.maxV = v
				}
			}
		}
	}
	if a.want["avg"] {
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
// entries are scanned once.
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
	need := map[int]bool{groupCol: true}
	for _, ga := range gas {
		if ga.Col >= 0 {
			need[ga.Col] = true
		}
	}
	plLower, plUpper := PKRangeBytes(r)
	var rows []sqlx.Row
	err = e.readFrom(e.store(t.engine), func(tx storeTx) error {
		ct, ok := tx.(*cstoreTx)
		if !ok {
			return errors.New("table " + table + " is not backed by the CSTORE store")
		}
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
		type grp struct {
			gval sqlx.Value
			accs []*colAccum
		}
		idx := map[string]*grp{}
		var order []*grp
		for j := 0; j < n; j++ {
			gv := scans[groupCol].vals[j]
			key := EncodePK([]sqlx.Value{toSQLValue(gv)})
			g := idx[key]
			if g == nil {
				g = &grp{gval: toSQLValue(gv), accs: make([]*colAccum, len(gas))}
				for k, ga := range gas {
					g.accs[k] = newColAccum(ga.Aggs)
				}
				idx[key] = g
				order = append(order, g)
			}
			for k, ga := range gas {
				if ga.Col < 0 {
					g.accs[k].star()
					continue
				}
				if err := g.accs[k].add(scans[ga.Col].vals[j]); err != nil {
					return err
				}
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
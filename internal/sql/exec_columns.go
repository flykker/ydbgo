package sql

import "strings"

// PKBound is one side of a range constraint on the leading PK columns.
// Prefix holds values for the first len(Prefix) PK columns (all but the last
// are equality constraints); the last element is the boundary value.
type PKBound struct {
	Prefix []Value // values for the leading PK columns
	Incl   bool    // inclusive (>= / <=); false means strict (>/<)
}

// PKRange constrains the leading PK columns of a scan. Rows outside
// [Lower, Upper) cannot match the WHERE clause the range was derived from, so
// it is safe to prune a scan to the range and evaluate the WHERE per row
// afterwards (the range is always a superset of the matching rows).
type PKRange struct {
	Lower *PKBound // nil = unbounded below
	Upper *PKBound // nil = unbounded above
}

// GroupAgg requests one aggregate over one column in a grouped (GROUP BY)
// columnar scan. Col -1 with Aggs ["count"] means COUNT(*) over each group.
type GroupAgg struct {
	Col  int      // schema column index
	Aggs []string // "count","sum","min","max","avg" (one entry per select item)
}

// ColumnEngine is an optional columnar capability implemented by CSTORE
// tables. It lets the executor project only the columns a SELECT touches, push
// whole-table aggregates down to the store (avoiding row reconstruction) and
// prune scans to a primary-key range derived from WHERE. r may be nil for an
// unbounded scan.
type ColumnEngine interface {
	// ScanColumns returns every row of the table with only the given columns
	// materialized (indexes into schema.Columns); other columns come back
	// Null. Rows are full-width and ordered like Engine.Scan.
	ScanColumns(table string, colIdx []int, r *PKRange) ([]Row, error)
	// ColumnCount returns the number of rows in the table.
	ColumnCount(table string, r *PKRange) (int64, error)
	// ColumnAggregate computes a whole-table aggregate over one column. agg is
	// one of "count", "sum", "min", "max", "avg".
	ColumnAggregate(table string, colIdx int, agg string, r *PKRange) (Value, error)
	// ColumnAggregates computes several whole-table aggregates over one column
	// in a single scan. aggs is a list of "count","sum","min","max","avg".
	ColumnAggregates(table string, colIdx int, aggs []string, r *PKRange) ([]Value, error)
	// ColumnGroupedAggregates computes aggregates per group of groupCol over a
	// single columnar pass, returning one row per group: [group value, the
	// aggregate results of gas in order]. All the aggregate columns are read
	// in one scan each (columns shared by several gas are read once).
	ColumnGroupedAggregates(table string, groupCol int, gas []GroupAgg, r *PKRange) ([]Row, error)
}

// rowsFor fetches the rows of a SELECT. For CSTORE tables it materializes only
// the columns the query touches (columnar projection) and prunes the scan to
// the primary-key range derived from WHERE; everything else uses a full scan.
func (ex *Executor) rowsFor(s *SelectStmt, schema *TableSchema) ([]Row, error) {
	ce, _ := ex.Eng.(ColumnEngine)
	if schema == nil || schema.Engine != "CSTORE" || ce == nil {
		return ex.Eng.Scan(s.From)
	}
	rng, _ := PKRangeFromWhere(schema, s.Where)
	idx, full := neededColumnIndexes(schema, s)
	if full {
		idx = make([]int, len(schema.Columns))
		for i := range schema.Columns {
			idx[i] = i
		}
	}
	return ce.ScanColumns(s.From, idx, rng)
}

// aggregatePushdown computes whole-table aggregates directly in the columnar
// store when a SELECT is a pure aggregate (no GROUP BY / ORDER BY / LIMIT; no
// WHERE, or a WHERE fully consumed as a primary-key range). Aggregates over
// the same column are combined into one columnar scan.
func (ex *Executor) aggregatePushdown(s *SelectStmt, schema *TableSchema) (*Result, bool, error) {
	if schema == nil || schema.Engine != "CSTORE" ||
		len(s.GroupBy) > 0 || len(s.OrderBy) > 0 || s.HasLimit {
		return nil, false, nil
	}
	var rng *PKRange
	if s.Where != nil {
		var exact bool
		rng, exact = PKRangeFromWhere(schema, s.Where)
		if !exact {
			return nil, false, nil
		}
	}
	ce, ok := ex.Eng.(ColumnEngine)
	if !ok {
		return nil, false, nil
	}
	type itemAgg struct {
		pos  int
		col  int // schema column index
		agg  string
		star bool // count(*)
	}
	// First pass: classify every select item.
	var itemAggs []itemAgg
	for i, it := range s.Items {
		call, ok := it.Expr.(*Call)
		if !ok {
			return nil, false, nil
		}
		name := strings.ToLower(call.Name)
		switch name {
		case "count":
			if len(call.Args) != 1 {
				return nil, false, nil
			}
			if id, ok := call.Args[0].(*Ident); ok && id.Name == "*" {
				itemAggs = append(itemAggs, itemAgg{pos: i, star: true})
				continue
			}
			ci, ok := columnIndexOf(schema, call.Args[0])
			if !ok {
				return nil, false, nil
			}
			itemAggs = append(itemAggs, itemAgg{pos: i, col: ci, agg: "count"})
		case "sum", "min", "max", "avg":
			if len(call.Args) != 1 {
				return nil, false, nil
			}
			ci, ok := columnIndexOf(schema, call.Args[0])
			if !ok {
				return nil, false, nil
			}
			itemAggs = append(itemAggs, itemAgg{pos: i, col: ci, agg: name})
		default:
			return nil, false, nil
		}
	}
	// Group by column and run one scan per column.
	row := make(Row, len(s.Items))
	byCol := map[int][]*itemAgg{}
	var colOrder []int
	for _, ia := range itemAggs {
		if ia.star {
			n, err := ce.ColumnCount(s.From, rng)
			if err != nil {
				return nil, false, err
			}
			row[ia.pos] = IntValue(n)
			continue
		}
		if _, ok := byCol[ia.col]; !ok {
			colOrder = append(colOrder, ia.col)
		}
		ia2 := ia
		byCol[ia.col] = append(byCol[ia.col], &ia2)
	}
	for _, ci := range colOrder {
		group := byCol[ci]
		aggs := make([]string, len(group))
		for j, ia := range group {
			aggs[j] = ia.agg
		}
		vals, err := ce.ColumnAggregates(s.From, ci, aggs, rng)
		if err != nil {
			return nil, false, err
		}
		for j, ia := range group {
			row[ia.pos] = vals[j]
		}
	}
	return &Result{Type: "select", Columns: resultColumns(s.Items), Rows: []Row{row}, Affected: 1}, true, nil
}

// groupedPushdown computes GROUP BY aggregates directly in the columnar store
// when a SELECT groups by a single column and every item is either that column
// or an aggregate call, with no ORDER BY. The WHERE (if any) must be fully
// consumed as a primary-key range.
func (ex *Executor) groupedPushdown(s *SelectStmt, schema *TableSchema) (*Result, bool, error) {
	if schema == nil || schema.Engine != "CSTORE" || len(s.GroupBy) != 1 ||
		len(s.OrderBy) > 0 {
		return nil, false, nil
	}
	gid, ok := columnIndexOf(schema, s.GroupBy[0])
	if !ok {
		return nil, false, nil
	}
	var rng *PKRange
	if s.Where != nil {
		var exact bool
		rng, exact = PKRangeFromWhere(schema, s.Where)
		if !exact {
			return nil, false, nil
		}
	}
	ce, ok := ex.Eng.(ColumnEngine)
	if !ok {
		return nil, false, nil
	}
	// Classify every item: either the group column itself or an aggregate.
	var gas []GroupAgg
	gasPos := make([]int, len(s.Items)) // item -> index into gas (-1 = group column)
	grpName := schema.Columns[gid].Name
	for i, it := range s.Items {
		if id, ok := it.Expr.(*Ident); ok && id.Name == grpName {
			gasPos[i] = -1
			continue
		}
		call, ok := it.Expr.(*Call)
		if !ok {
			return nil, false, nil
		}
		name := strings.ToLower(call.Name)
		var col int
		if name == "count" && len(call.Args) == 1 {
			if id, ok := call.Args[0].(*Ident); ok && id.Name == "*" {
				col = -1
			} else {
				ci, ok2 := columnIndexOf(schema, call.Args[0])
				if !ok2 {
					return nil, false, nil
				}
				col = ci
			}
		} else {
			switch name {
			case "sum", "min", "max", "avg":
				if len(call.Args) != 1 {
					return nil, false, nil
				}
				ci, ok2 := columnIndexOf(schema, call.Args[0])
				if !ok2 {
					return nil, false, nil
				}
				col = ci
			default:
				return nil, false, nil
			}
		}
		gasPos[i] = len(gas)
		gas = append(gas, GroupAgg{Col: col, Aggs: []string{name}})
	}
	rows, err := ce.ColumnGroupedAggregates(s.From, gid, gas, rng)
	if err != nil {
		return nil, false, err
	}
	out := make([]Row, 0, len(rows))
	for _, gr := range rows {
		row := make(Row, len(s.Items))
		for i, p := range gasPos {
			if p < 0 {
				row[i] = gr[0]
				continue
			}
			row[i] = gr[1+p]
		}
		out = append(out, row)
	}
	if s.Distinct {
		seen := map[string]bool{}
		filtered := out[:0]
		for _, r := range out {
			k := rowKeyString(r)
			if !seen[k] {
				seen[k] = true
				filtered = append(filtered, r)
			}
		}
		out = filtered
	}
	if s.HasLimit && int(s.Limit) < len(out) {
		out = out[:s.Limit]
	}
	return &Result{Type: "select", Columns: resultColumns(s.Items), Rows: out, Affected: int64(len(out))}, true, nil
}

// resultColumns derives the output column names of a SELECT.
func resultColumns(items []*SelectItem) []string {
	cols := make([]string, len(items))
	for i, it := range items {
		if it.Alias != "" {
			cols[i] = it.Alias
		} else if id, ok := it.Expr.(*Ident); ok {
			cols[i] = id.Name
		} else {
			cols[i] = "col" + string(rune('0'+i))
		}
	}
	return cols
}

type aggItemKind int

const (
	aggPlain aggItemKind = iota // count/sum/min/max: single mergeable sub
	aggAvg                      // avg: composed from sum + count subs
)

// aggSub is one mergeable partial aggregate a shard computes.
type aggSub struct {
	kind string // "count","sum","min","max"
	col  string // column name; "" for count(*)
}

// AggregatePlan rewrites a pure whole-table aggregate SELECT for sharded
// pushdown: each shard computes mergeable partial aggregates over its own
// range and the coordinator merges them into the final result row.
type AggregatePlan struct {
	partialExprs []Expr   // shard SELECT items (partial aggregates)
	mergeKinds   []string // parallel to partialExprs
	partialCols  []string // parallel to partialExprs: target column name ("" = count(*))
	itemKinds    []aggItemKind
	itemSubIdx   [][]int  // per output item: indexes into partial results
	cols         []string // output column names
}

// PlanAggregate builds an AggregatePlan for a SELECT whose items are all
// aggregate calls and which has no GROUP BY / ORDER BY / LIMIT / DISTINCT.
func PlanAggregate(s *SelectStmt, schema *TableSchema) (*AggregatePlan, bool) {
	if schema == nil || len(s.GroupBy) > 0 || len(s.OrderBy) > 0 || s.HasLimit || s.Distinct {
		return nil, false
	}
	p := &AggregatePlan{
		itemKinds:  make([]aggItemKind, len(s.Items)),
		itemSubIdx: make([][]int, len(s.Items)),
		cols:       resultColumns(s.Items),
	}
	for i, it := range s.Items {
		if !appendAggItem(p, i, it, schema) {
			return nil, false
		}
	}
	return p, true
}

// appendAggItem plans one aggregate select item, appending its mergeable
// partials to p. Returns false when the item is not a supported aggregate.
func appendAggItem(p *AggregatePlan, i int, it *SelectItem, schema *TableSchema) bool {
	call, ok := it.Expr.(*Call)
	if !ok {
		return false
	}
	name := strings.ToLower(call.Name)
	if len(call.Args) != 1 {
		return false
	}
	var colName string
	if name != "count" {
		ci, ok := columnIndexOf(schema, call.Args[0])
		if !ok {
			return false
		}
		colName = schema.Columns[ci].Name
	} else if id, ok := call.Args[0].(*Ident); ok && id.Name == "*" {
		colName = ""
	} else {
		ci, ok := columnIndexOf(schema, call.Args[0])
		if !ok {
			return false
		}
		colName = schema.Columns[ci].Name
	}
	switch name {
	case "count", "sum", "min", "max":
		p.itemKinds[i] = aggPlain
		p.itemSubIdx[i] = []int{len(p.partialExprs)}
		p.partialExprs = append(p.partialExprs, aggCallExpr(name, colName))
		p.mergeKinds = append(p.mergeKinds, name)
		p.partialCols = append(p.partialCols, colName)
	case "avg":
		p.itemKinds[i] = aggAvg
		p.itemSubIdx[i] = []int{len(p.partialExprs), len(p.partialExprs) + 1}
		p.partialExprs = append(p.partialExprs, aggCallExpr("sum", colName))
		p.partialExprs = append(p.partialExprs, aggCallExpr("count", colName))
		p.mergeKinds = append(p.mergeKinds, "sum", "count")
		p.partialCols = append(p.partialCols, colName, colName)
	default:
		return false
	}
	return true
}

func aggCallExpr(name, col string) Expr {
	if name == "count" && col == "" {
		return &Call{Name: "count", Args: []Expr{&Ident{Name: "*"}}}
	}
	return &Call{Name: name, Args: []Expr{&Ident{Name: col}}}
}

// ShardSQL renders the partial-aggregate SELECT a shard should execute.
// whereSQL is the optional " WHERE ..." clause to append.
func (p *AggregatePlan) ShardSQL(table, whereSQL string) string {
	parts := make([]string, len(p.partialExprs))
	for i, e := range p.partialExprs {
		parts[i] = ExprString(e)
	}
	return "SELECT " + strings.Join(parts, ", ") + " FROM " + table + whereSQL
}

// Merge combines one partial-aggregate row from each shard into the final
// result row (in the original select-item order).
func (p *AggregatePlan) Merge(partials []Row) Row {
	merged := make([]Value, len(p.partialExprs))
	for i := range p.partialExprs {
		merged[i] = mergeAggSub(p.mergeKinds[i], partials, i)
	}
	out := make(Row, len(p.itemKinds))
	for i := range out {
		if p.itemKinds[i] == aggAvg {
			sumV := merged[p.itemSubIdx[i][0]]
			cntV := merged[p.itemSubIdx[i][1]]
			if sumV.Null || cntV.Null || cntV.Int == 0 {
				out[i] = NullValue
			} else {
				f, _ := sumV.AsFloat()
				out[i] = FloatValue(f / float64(cntV.Int))
			}
			continue
		}
		out[i] = merged[p.itemSubIdx[i][0]]
	}
	return out
}

// Cols returns the output column names of the original SELECT.
func (p *AggregatePlan) Cols() []string { return p.cols }

// PartialTypes returns the value type of each shard partial column, in the
// same order as the shard's SELECT items, so remote partial rows that arrive
// as strings can be reconstructed as typed values.
func (p *AggregatePlan) PartialTypes(schema *TableSchema) []Type {
	colType := map[string]Type{}
	for _, c := range schema.Columns {
		colType[c.Name] = c.Type
	}
	types := make([]Type, len(p.partialExprs))
	for i, k := range p.mergeKinds {
		if k == "count" {
			types[i] = TypeInt
			continue
		}
		types[i] = colType[p.partialCols[i]]
	}
	return types
}

// GroupedPlan rewrites a single-column GROUP BY aggregate SELECT for sharded
// pushdown: each shard computes its partial groups (group value + mergeable
// partial aggregates) and the coordinator merges groups with the same value.
type GroupedPlan struct {
	agg      *AggregatePlan
	groupCol string
}

// PlanGrouped builds a GroupedPlan for a SELECT with exactly one GROUP BY
// column and no ORDER BY / LIMIT / DISTINCT, whose items are the group column
// or supported aggregate calls.
func PlanGrouped(s *SelectStmt, schema *TableSchema) (*GroupedPlan, bool) {
	if schema == nil || len(s.GroupBy) != 1 || len(s.OrderBy) > 0 || s.HasLimit || s.Distinct {
		return nil, false
	}
	gid, ok := columnIndexOf(schema, s.GroupBy[0])
	if !ok {
		return nil, false
	}
	grpName := schema.Columns[gid].Name
	p := &AggregatePlan{
		itemKinds:  make([]aggItemKind, len(s.Items)),
		itemSubIdx: make([][]int, len(s.Items)),
		cols:       resultColumns(s.Items),
	}
	for i, it := range s.Items {
		if id, ok := it.Expr.(*Ident); ok && id.Name == grpName {
			p.itemSubIdx[i] = nil // the group column: output = group value
			continue
		}
		if !appendAggItem(p, i, it, schema) {
			return nil, false
		}
	}
	return &GroupedPlan{agg: p, groupCol: grpName}, true
}

// ShardSQL renders the partial-group SELECT a shard should execute: group
// value plus each partial aggregate, grouped by the group column.
func (g *GroupedPlan) ShardSQL(table, whereSQL string) string {
	parts := make([]string, 0, len(g.agg.partialExprs)+1)
	parts = append(parts, g.groupCol)
	for _, e := range g.agg.partialExprs {
		parts = append(parts, ExprString(e))
	}
	return "SELECT " + strings.Join(parts, ", ") + " FROM " + table + whereSQL + " GROUP BY " + g.groupCol
}

// Cols returns the output column names of the original SELECT.
func (g *GroupedPlan) Cols() []string { return g.agg.cols }

// PartialTypes returns the value type of each shard partial column (group
// value first, then the partial aggregates).
func (g *GroupedPlan) PartialTypes(schema *TableSchema) []Type {
	types := make([]Type, 0, len(g.agg.partialExprs)+1)
	gt := TypeString
	for _, c := range schema.Columns {
		if c.Name == g.groupCol {
			gt = c.Type
			break
		}
	}
	return append(append(types, gt), g.agg.PartialTypes(schema)...)
}

// Merge combines partial-group rows from every shard into the final groups:
// rows with the same group value are merged (sums summed, counts counted,
// min/max folded, avg re-weighted), preserving first-seen group order.
func (g *GroupedPlan) Merge(partials []Row) []Row {
	type bucket struct {
		gval Value
		rows []Row
	}
	idx := map[string]*bucket{}
	var order []*bucket
	for _, r := range partials {
		if len(r) == 0 {
			continue
		}
		key := r[0].String()
		b := idx[key]
		if b == nil {
			b = &bucket{gval: r[0]}
			idx[key] = b
			order = append(order, b)
		}
		b.rows = append(b.rows, r)
	}
	out := make([]Row, 0, len(order))
	for _, b := range order {
		row := make(Row, len(g.agg.itemKinds))
		for i := range row {
			subIdx := g.agg.itemSubIdx[i]
			if subIdx == nil {
				row[i] = b.gval
				continue
			}
			if g.agg.itemKinds[i] == aggAvg {
				sumV := mergeAggSub("sum", b.rows, subIdx[0]+1)
				cntV := mergeAggSub("count", b.rows, subIdx[1]+1)
				if sumV.Null || cntV.Null || cntV.Int == 0 {
					row[i] = NullValue
				} else {
					f, _ := sumV.AsFloat()
					row[i] = FloatValue(f / float64(cntV.Int))
				}
				continue
			}
			row[i] = mergeAggSub(g.agg.mergeKinds[subIdx[0]], b.rows, subIdx[0]+1)
		}
		out = append(out, row)
	}
	return out
}

func mergeAggSub(kind string, partials []Row, pos int) Value {
	switch kind {
	case "count":
		var total int64
		for _, r := range partials {
			if pos < len(r) && !r[pos].Null {
				total += r[pos].Int
			}
		}
		return IntValue(total)
	case "sum":
		var total float64
		allInt, any := true, false
		for _, r := range partials {
			if pos >= len(r) || r[pos].Null {
				continue
			}
			if r[pos].Type != TypeInt {
				allInt = false
			}
			f, _ := r[pos].AsFloat()
			total += f
			any = true
		}
		if !any {
			return NullValue
		}
		if allInt {
			return IntValue(int64(total))
		}
		return FloatValue(total)
	case "min", "max":
		var best Value
		bestSet := false
		for _, r := range partials {
			if pos >= len(r) || r[pos].Null {
				continue
			}
			if !bestSet {
				best, bestSet = r[pos], true
				continue
			}
			c, err := Compare(r[pos], best)
			if err != nil {
				continue
			}
			if (kind == "min" && c < 0) || (kind == "max" && c > 0) {
				best = r[pos]
			}
		}
		if !bestSet {
			return NullValue
		}
		return best
	}
	return NullValue
}

// PKRangeFromWhere extracts a range constraint on the leading PK columns from a
// WHERE clause made of AND-connected constant comparisons against PK columns.
// exact reports whether every leaf of the WHERE was consumed as such a
// constraint, in which case the rows inside the range are exactly the rows that
// pass the WHERE (safe for whole-table aggregate pushdown). The range itself is
// always a superset of the matching rows, so it is always safe to prune with
// it; a nil range means no pruning is possible.
func PKRangeFromWhere(schema *TableSchema, where Expr) (rng *PKRange, exact bool) {
	if schema == nil || len(schema.PK) == 0 || where == nil {
		return nil, false
	}
	pos := make(map[string]int, len(schema.PK))
	colTypes := make([]Type, len(schema.PK))
	for i, name := range schema.PK {
		pos[name] = i
		for _, c := range schema.Columns {
			if c.Name == name {
				colTypes[i] = c.Type
				break
			}
		}
	}
	type constraint struct {
		eq       Value
		hasEq    bool
		lo       Value
		loStrict bool
		hasLo    bool
		up       Value
		upStrict bool
		hasUp    bool
	}
	cons := map[int]*constraint{}
	exact = true
	var visit func(e Expr)
	visit = func(e Expr) {
		bin, ok := e.(*BinaryOp)
		if !ok {
			exact = false
			return
		}
		op := strings.ToUpper(bin.Op)
		if op == "AND" {
			visit(bin.Left)
			visit(bin.Right)
			return
		}
		if op == "OR" {
			exact = false
			return
		}
		// comparison: <ident> <op> <literal> or <literal> <op> <ident>
		var id *Ident
		var lit *Literal
		switch l := bin.Left.(type) {
		case *Ident:
			if r, ok := bin.Right.(*Literal); ok {
				id, lit = l, r
			}
		case *Literal:
			if r, ok := bin.Right.(*Ident); ok {
				id, lit = r, l
				switch op {
				case "<":
					op = ">"
				case "<=":
					op = ">="
				case ">":
					op = "<"
				case ">=":
					op = "<="
				}
			}
		}
		if id == nil {
			exact = false
			return
		}
		p, isPK := pos[id.Name]
		if !isPK {
			exact = false
			return
		}
		// LIKE on a string PK: a pattern with a leading literal run folds into a
		// PK range [prefix, successor(prefix)). With no wildcards it degenerates
		// to equality; a lone trailing '%' makes the range exact.
		if op == "LIKE" {
			if colTypes[p] != TypeString || lit == nil || lit.Type != TypeString {
				exact = false
				return
			}
			pat := lit.Str
			wi := -1
			for i := 0; i < len(pat); i++ {
				if pat[i] == '%' || pat[i] == '_' {
					wi = i
					break
				}
			}
			c := cons[p]
			if c == nil {
				c = &constraint{}
				cons[p] = c
			}
			if wi < 0 {
				c.eq, c.hasEq = StrValue(pat), true
				return
			}
			if wi == 0 {
				exact = false // leading wildcard cannot be folded into a range
				return
			}
			prefix := pat[:wi]
			c.lo, c.loStrict, c.hasLo = StrValue(prefix), false, true
			c.up, c.upStrict, c.hasUp = StrValue(successor(prefix)), false, true
			if pat[wi:] != "%" {
				exact = false // wildcards after the prefix need a residual filter
			}
			return
		}
		v, err := Eval(lit, nil)
		if err != nil {
			exact = false
			return
		}
		if v.Null {
			exact = false
			return
		}
		// Normalize the literal to the PK column's type so the encoded bound
		// sorts identically to stored values (e.g. '2024-...' string literal
		// against a timestamp PK column).
		if ct := colTypes[p]; ct != TypeNull && v.Type != ct {
			converted, cerr := Convert(v, ct)
			if cerr != nil {
				exact = false
				return
			}
			v = converted
		}
		c := cons[p]
		if c == nil {
			c = &constraint{}
			cons[p] = c
		}
		switch op {
		case "=":
			c.eq, c.hasEq = v, true
		case ">", ">=":
			if !c.hasLo {
				c.lo, c.loStrict, c.hasLo = v, op == ">", true
				break
			}
			if cmp, err := Compare(v, c.lo); err == nil && cmp > 0 {
				c.lo, c.loStrict = v, op == ">"
			} else if err == nil && cmp == 0 && op == ">" {
				c.loStrict = true
			}
		case "<", "<=":
			if !c.hasUp {
				c.up, c.upStrict, c.hasUp = v, op == "<", true
				break
			}
			if cmp, err := Compare(v, c.up); err == nil && cmp < 0 {
				c.up, c.upStrict = v, op == "<"
			} else if err == nil && cmp == 0 && op == "<" {
				c.upStrict = true
			}
		}
	}
	visit(where)
	if len(cons) == 0 {
		return nil, exact
	}

	prefix := []Value{}
	for i := range schema.PK {
		c := cons[i]
		if c == nil {
			// unconstrained column: the usable range ends at the equality
			// prefix; any constraint on a later column cannot be folded in
			for j := i + 1; j < len(schema.PK); j++ {
				if cons[j] != nil {
					exact = false
					break
				}
			}
			if len(prefix) > 0 {
				return &PKRange{
					Lower: &PKBound{Prefix: prefix, Incl: true},
					Upper: &PKBound{Prefix: prefix, Incl: true},
				}, exact
			}
			return nil, exact
		}
		if c.hasEq {
			if c.hasLo || c.hasUp {
				exact = false // range constraint on the same column dropped
			}
			prefix = append(prefix, c.eq)
			if i == len(schema.PK)-1 {
				return &PKRange{
					Lower: &PKBound{Prefix: prefix, Incl: true},
					Upper: &PKBound{Prefix: prefix, Incl: true},
				}, exact
			}
			continue
		}
		// boundary column with a range constraint; constraints on later
		// columns cannot be expressed as a single prefix range
		for j := i + 1; j < len(schema.PK); j++ {
			if cons[j] != nil {
				exact = false
				break
			}
		}
		r := &PKRange{}
		if c.hasLo {
			r.Lower = &PKBound{Prefix: appendPrefix(prefix, c.lo), Incl: !c.loStrict}
		}
		if c.hasUp {
			r.Upper = &PKBound{Prefix: appendPrefix(prefix, c.up), Incl: !c.upStrict}
		}
		return r, exact
	}
	return nil, exact
}

func appendPrefix(prefix []Value, v Value) []Value {
	out := make([]Value, len(prefix)+1)
	copy(out, prefix)
	out[len(prefix)] = v
	return out
}

// successor returns the smallest byte-lexicographic string strictly greater
// than s: the last non-0xff byte is incremented, or a 0x00 byte is appended
// when every byte is 0xff. Used to build the exclusive upper bound of a
// prefix match on a string PK.
func successor(s string) string {
	b := []byte(s)
	i := len(b) - 1
	for i >= 0 && b[i] == 0xff {
		i--
	}
	if i < 0 {
		return s + "\x00"
	}
	b[i]++
	return string(b[:i+1])
}

// columnIndexOf resolves a column reference to its schema index.
func columnIndexOf(schema *TableSchema, e Expr) (int, bool) {
	id, ok := e.(*Ident)
	if !ok {
		return 0, false
	}
	for i, c := range schema.Columns {
		if c.Name == id.Name {
			return i, true
		}
	}
	return 0, false
}

// referencedColumns returns the set of column names a SELECT touches: select
// items, WHERE, GROUP BY and ORDER BY expressions. A "*" Ident marks the
// whole-row wildcard.
func referencedColumns(s *SelectStmt) map[string]bool {
	refs := map[string]bool{}
	var walk func(e Expr)
	walk = func(e Expr) {
		switch n := e.(type) {
		case *Ident:
			refs[n.Name] = true
		case *Literal:
		case *BinaryOp:
			walk(n.Left)
			walk(n.Right)
		case *UnaryOp:
			walk(n.Expr)
		case *Call:
			for _, a := range n.Args {
				walk(a)
			}
		case *CastExpr:
			walk(n.Expr)
		}
	}
	for _, it := range s.Items {
		walk(it.Expr)
	}
	if s.Where != nil {
		walk(s.Where)
	}
	for _, g := range s.GroupBy {
		walk(g)
	}
	for _, o := range s.OrderBy {
		walk(o.Expr)
	}
	return refs
}

// neededColumnIndexes maps the columns a SELECT references onto schema column
// indexes. full is true when every column must be materialized (e.g. *).
func neededColumnIndexes(schema *TableSchema, s *SelectStmt) (idx []int, full bool) {
	refs := referencedColumns(s)
	if refs["*"] {
		return nil, true
	}
	for i, c := range schema.Columns {
		if refs[c.Name] {
			idx = append(idx, i)
		}
	}
	if len(idx) == len(schema.Columns) {
		return nil, true
	}
	return idx, false
}

// ProjectionColumns returns the columns a SELECT needs materialized, always
// including PK columns (they are required to deduplicate across shard
// boundaries). full is true when every column is needed.
func ProjectionColumns(schema *TableSchema, s *SelectStmt) ([]string, bool) {
	refs := referencedColumns(s)
	if refs["*"] {
		return nil, true
	}
	seen := map[string]bool{}
	var cols []string
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			cols = append(cols, name)
		}
	}
	for _, c := range schema.Columns {
		if refs[c.Name] {
			add(c.Name)
		}
	}
	for _, p := range schema.PK {
		add(p)
	}
	if len(cols) == len(schema.Columns) {
		return nil, true
	}
	return cols, false
}

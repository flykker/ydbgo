package sql

import (
	"log"
	"time"
)

// Row is an ordered slice of values.
type Row []Value

// TableSchema describes a table's columns.
type TableSchema struct {
	Name      string
	Columns   []ColumnDef
	PK        []string
	Engine    string        // storage engine: "TABLE", "KV", "CSTORE" ("" = TABLE)
	Retention time.Duration // rows older than now-retention are auto-deleted (0 = disabled)
}

// Engine is the storage interface used by the executor.
// Implemented by storage.Engine.
type Engine interface {
	CreateTable(schema *TableSchema) error
	DropTable(name string) error
	GetSchema(name string) (*TableSchema, error)
	Insert(table string, row map[string]Value) error
	Scan(table string) ([]Row, error)
	Get(table string, pkValues []Value) (Row, error)
	Update(table string, pkValues []Value, set map[string]Value) error
	Delete(table string, pkValues []Value) error
	// DeleteRange removes every row whose encoded PK falls inside the range
	// (nil bounds = unbounded). Only CSTORE tables support range deletion.
	DeleteRange(table string, r *PKRange) (int64, error)
}

// KVEngine is the raw byte-KV surface for ENGINE=KV tables. It is an optional
// capability: engines that do not back a KV layout (union/empty engines used
// for sharded scatter) do not implement it and KV statements fail cleanly.
type KVEngine interface {
	KVPut(table string, key, value string) error
	KVGet(table string, key string) (Value, error)
	KVDelete(table string, key string) error
	KVScan(table string, start, end string) ([]KVEntry, error)
}

// IndexEngine is the optional secondary-index surface of a storage engine.
// Engines that cannot build indexes (union/empty engines used for sharded
// scatter) do not implement it and index DDL fails cleanly.
type IndexEngine interface {
	CreateIndex(table, name string, cols []string, ifNotExists bool) error
	DropIndex(table, name string, ifExists bool) error
}

// BatchInsertEngine is an optional engine capability: inserting many rows of
// one table inside a single transaction (one write lock, one store commit).
type BatchInsertEngine interface {
	BatchInsert(table string, rows []map[string]Value) (int64, error)
}

// BatchRowsEngine is an optional engine capability like BatchInsertEngine but
// taking rows in schema column order (a Row is index-aligned with
// schema.Columns), avoiding the per-row map allocation BatchInsert performs.
type BatchRowsEngine interface {
	BatchInsertRows(table string, rows []Row) (int64, error)
}

// ConstRangeUpdater is an optional engine capability: assigning constant
// values to columns of every row whose encoded PK falls inside a range,
// streaming the merged PK column and writing pre-encoded cells directly
// instead of materializing rows (vectorized constant-SET UPDATE). The bool
// result reports whether the table shape was supported; false makes callers
// fall back to the generic scan-and-rewrite path.
type ConstRangeUpdater interface {
	UpdateColumnConst(table string, consts map[int]Value, r *PKRange) (int64, bool, error)
}

// KVEntry is one raw key/value pair returned by a KV SCAN.
type KVEntry struct {
	Key   string
	Value string
}

// Result is the outcome of executing a statement.
type Result struct {
	Columns  []string // column names (SELECT)
	Rows     []Row    // result rows (SELECT)
	Affected int64    // affected rows for DML
	Type     string   // "select","insert","update","delete","create_table","drop_table","begin","commit","rollback","create_index","drop_index","create_database","kv_put","kv_get","kv_delete","kv_scan"
}

// Executor executes parsed statements against an Engine.
type Executor struct {
	Eng Engine
}

func NewExecutor(e Engine) *Executor { return &Executor{Eng: e} }

// Execute runs a single statement, returning a result.
func (ex *Executor) Execute(st Statement) (*Result, error) {
	switch s := st.(type) {
	case *CreateTableStmt:
		return ex.execCreateTable(s)
	case *DropTableStmt:
		return ex.execDropTable(s)
	case *CreateIndexStmt:
		return ex.execCreateIndex(s)
	case *DropIndexStmt:
		return ex.execDropIndex(s)
	case *InsertStmt:
		return ex.execInsert(s)
	case *UpdateStmt:
		return ex.execUpdate(s)
	case *DeleteStmt:
		return ex.execDelete(s)
	case *SelectStmt:
		return ex.execSelect(s)
	case *BeginStmt:
		return &Result{Type: "begin"}, nil
	case *CommitStmt:
		return &Result{Type: "commit"}, nil
	case *RollbackStmt:
		return &Result{Type: "rollback"}, nil
	case *CreateDatabaseStmt:
		return &Result{Type: "create_database"}, nil
	case *KVPutStmt:
		return ex.execKVPut(s)
	case *KVGetStmt:
		return ex.execKVGet(s)
	case *KVDeleteStmt:
		return ex.execKVDelete(s)
	case *KVScanStmt:
		return ex.execKVScan(s)
	}
	return nil, &ExecError{Msg: "unsupported statement"}
}

func (ex *Executor) kvEngine() (KVEngine, error) {
	ke, ok := ex.Eng.(KVEngine)
	if !ok {
		return nil, &ExecError{Msg: "engine does not support raw KV operations"}
	}
	return ke, nil
}

func (ex *Executor) indexEngine() (IndexEngine, error) {
	ie, ok := ex.Eng.(IndexEngine)
	if !ok {
		return nil, &ExecError{Msg: "engine does not support secondary indexes"}
	}
	return ie, nil
}

func (ex *Executor) execCreateIndex(s *CreateIndexStmt) (*Result, error) {
	ie, err := ex.indexEngine()
	if err != nil {
		return nil, err
	}
	if err := ie.CreateIndex(s.Table, s.Name, s.Columns, s.IfNotExists); err != nil {
		return nil, err
	}
	return &Result{Type: "create_index", Affected: 1}, nil
}

func (ex *Executor) execDropIndex(s *DropIndexStmt) (*Result, error) {
	ie, err := ex.indexEngine()
	if err != nil {
		return nil, err
	}
	if err := ie.DropIndex(s.Table, s.Name, s.IfExists); err != nil {
		return nil, err
	}
	return &Result{Type: "drop_index", Affected: 1}, nil
}

func (ex *Executor) execKVPut(s *KVPutStmt) (*Result, error) {
	ke, err := ex.kvEngine()
	if err != nil {
		return nil, err
	}
	if err := ke.KVPut(s.Table, s.Key, s.Value); err != nil {
		return nil, err
	}
	return &Result{Type: "kv_put", Affected: 1}, nil
}

func (ex *Executor) execKVGet(s *KVGetStmt) (*Result, error) {
	ke, err := ex.kvEngine()
	if err != nil {
		return nil, err
	}
	v, err := ke.KVGet(s.Table, s.Key)
	if err != nil {
		return nil, err
	}
	if v.Null {
		return &Result{Type: "kv_get", Columns: []string{"key", "value"}, Rows: nil}, nil
	}
	return &Result{Type: "kv_get", Columns: []string{"key", "value"}, Rows: []Row{{StrValue(s.Key), v}}}, nil
}

func (ex *Executor) execKVDelete(s *KVDeleteStmt) (*Result, error) {
	ke, err := ex.kvEngine()
	if err != nil {
		return nil, err
	}
	if err := ke.KVDelete(s.Table, s.Key); err != nil {
		return nil, err
	}
	return &Result{Type: "kv_delete", Affected: 1}, nil
}

func (ex *Executor) execKVScan(s *KVScanStmt) (*Result, error) {
	ke, err := ex.kvEngine()
	if err != nil {
		return nil, err
	}
	entries, err := ke.KVScan(s.Table, s.Start, s.End)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, Row{StrValue(e.Key), StrValue(e.Value)})
	}
	return &Result{Type: "kv_scan", Columns: []string{"key", "value"}, Rows: rows, Affected: int64(len(rows))}, nil
}

type ExecError struct{ Msg string }

func (e *ExecError) Error() string { return e.Msg }

// engineOf normalizes the ENGINE clause to its canonical uppercase name.
func engineOf(e string) string {
	switch e {
	case "kv":
		return "KV"
	case "cstore":
		return "CSTORE"
	case "cstore2":
		return "CSTORE2"
	default:
		return "TABLE"
	}
}

// IsColumnarEngine reports whether a normalized engine name is a columnar
// storage engine (CSTORE or CSTORE2), which unlocks columnar scans, projection
// and aggregate pushdown.
func IsColumnarEngine(engine string) bool {
	return engine == "CSTORE" || engine == "CSTORE2"
}

func (ex *Executor) execCreateTable(s *CreateTableStmt) (*Result, error) {
	if ex.Eng == nil {
		return nil, &ExecError{Msg: "no engine"}
	}
	cols := s.Columns
	if s.PK != nil && len(s.PK) > 0 {
		for i := range cols {
			for _, p := range s.PK {
				if cols[i].Name == p {
					cols[i].AsPrimary = true
					cols[i].NotNull = true
				}
			}
		}
	}
	// If no PK, promote first column as primary.
	hasPK := false
	for _, c := range cols {
		if c.AsPrimary {
			hasPK = true
			break
		}
	}
	pk := []string{}
	if !hasPK && len(cols) > 0 {
		cols[0].AsPrimary = true
		cols[0].NotNull = true
	}
	for _, c := range cols {
		if c.AsPrimary {
			pk = append(pk, c.Name)
		}
	}
	schema := &TableSchema{Name: s.Name, Columns: cols, PK: pk, Engine: engineOf(s.Engine), Retention: s.Retention}
	if err := ex.Eng.CreateTable(schema); err != nil {
		return nil, err
	}
	return &Result{Type: "create_table", Affected: 1}, nil
}

func (ex *Executor) execDropTable(s *DropTableStmt) (*Result, error) {
	if err := ex.Eng.DropTable(s.Name); err != nil {
		if s.IfExists && isNotFound(err) {
			return &Result{Type: "drop_table"}, nil
		}
		return nil, err
	}
	return &Result{Type: "drop_table", Affected: 1}, nil
}

func (ex *Executor) execInsert(s *InsertStmt) (*Result, error) {
	schema, err := ex.Eng.GetSchema(s.Table)
	if err != nil {
		return nil, err
	}
	cols := s.Columns
	colIndex := make(map[string]int)
	if len(cols) == 0 {
		// all columns in schema order
		for i, c := range schema.Columns {
			colIndex[c.Name] = i
		}
	} else {
		for i, c := range cols {
			colIndex[c] = i
		}
	}
	rows := make([]map[string]Value, 0, len(s.Rows))
	for _, row := range s.Rows {
		if len(row) < len(colIndex) {
			return nil, &ExecError{Msg: "row has fewer values than columns"}
		}
		vals := make(map[string]Value, len(schema.Columns))
		for _, c := range schema.Columns {
			idx, ok := colIndex[c.Name]
			if !ok {
				// use default or zero value
				vals[c.Name] = zeroValue(c)
				continue
			}
			v, err := Eval(row[idx], nil)
			if err != nil {
				return nil, err
			}
			vals[c.Name], err = Convert(v, c.Type)
			if err != nil {
				return nil, err
			}
			if c.NotNull && vals[c.Name].Null {
				return nil, &ExecError{Msg: "column " + c.Name + " is not null"}
			}
		}
		rows = append(rows, vals)
	}
	// Batch path: normalize once, insert everything in one transaction.
	if be, ok := ex.Eng.(BatchInsertEngine); ok {
		affected, err := be.BatchInsert(s.Table, rows)
		if err != nil {
			return nil, err
		}
		return &Result{Type: "insert", Affected: affected}, nil
	}
	affected := int64(0)
	for _, vals := range rows {
		if err := ex.Eng.Insert(s.Table, vals); err != nil {
			return nil, err
		}
		affected++
	}
	return &Result{Type: "insert", Affected: affected}, nil
}

func (ex *Executor) execUpdate(s *UpdateStmt) (*Result, error) {
	schema, err := ex.Eng.GetSchema(s.Table)
	if err != nil {
		return nil, err
	}
	// Point fast path: a WHERE that pins the complete primary key reads and
	// updates the single row directly instead of scanning the whole table.
	if rng, exact := PKRangeFromWhere(schema, s.Where); exact && rng != nil &&
		rng.Lower != nil && rng.Upper != nil && pkEqualBound(rng.Lower, rng.Upper) {
		pk := append([]Value(nil), rng.Lower.Prefix...)
		row, err := ex.Eng.Get(s.Table, pk)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return &Result{Type: "update", Affected: 0}, nil
		}
		ctx := rowContext(schema, row)
		set, err := evalSetsCtx(schema, s.Sets, ctx)
		if err != nil {
			return nil, err
		}
		if err := ex.Eng.Update(s.Table, pk, set); err != nil {
			return nil, err
		}
		return &Result{Type: "update", Affected: 1}, nil
	}
	// Columnar range fast path: for a CSTORE table a WHERE that folds into a
	// primary-key range reads only the rows inside that range columnar (one
	// granule-cached pass, no per-row decode) and writes every affected row
	// back in a single transaction. This is the ClickHouse mutation model:
	// rewrite the affected rows, not the whole table, with no per-row
	// Get/Update round trip.
	if IsColumnarEngine(schema.Engine) {
		if ce, ok := ex.Eng.(ColumnEngine); ok {
			rng, exact := PKRangeFromWhere(schema, s.Where)
			return ex.execUpdateColumnar(s, schema, ce, rng, exact)
		}
	}
	return ex.execUpdateScan(s, schema)
}

// execUpdateColumnar implements UPDATE over a columnar table. It scans only the
// rows inside the PK range derived from WHERE (a superset when the WHERE mixes
// PK and non-PK predicates; the residual WHERE is re-evaluated per row only in
// that case), then writes every affected row back in a single transaction.
//
// CH partial-merge: when the engine supports batch rows, only the columns the
// statement actually needs are read (PK columns for the keys, plus every column
// referenced by the SET expressions or the WHERE predicate), and only the
// columns assigned by SET are written. Every other column is marked Missing so
// the storage inherits its value from the previous row version instead of
// rewriting it.
func (ex *Executor) execUpdateColumnar(s *UpdateStmt, schema *TableSchema, ce ColumnEngine, rng *PKRange, rngExact bool) (*Result, error) {
	// Rewriting a PK column changes the row's key (delete + reinsert); the
	// batch upsert below keys rows by PK, so fall back to the generic path.
	for _, item := range s.Sets {
		for _, c := range pkIndex(schema) {
			if item.Column == c {
				return ex.execUpdateScan(s, schema)
			}
		}
	}
	// Vectorized constant-SET fast path: when WHERE folds exactly into the PK
	// range and every assignment is a literal, no rows are materialized — the
	// storage streams the merged PK column and each assigned cell is encoded
	// once and shared by every rewritten key (CH rewrites a mutation's target
	// columns the same way).
	if rngExact {
		if cru, ok := ex.Eng.(ConstRangeUpdater); ok {
			if consts, allLit := updateConstSets(schema, s.Sets); allLit {
				n, handled, uerr := cru.UpdateColumnConst(s.Table, consts, rng)
				if uerr != nil {
					return nil, uerr
				}
				if handled {
					return &Result{Type: "update", Affected: n}, nil
				}
			}
		}
	}
	br, rowsOK := ex.Eng.(BatchRowsEngine)
	be, batchOK := ex.Eng.(BatchInsertEngine)
	// CH partial-merge: when the engine accepts schema-ordered rows, read only
	// the columns the statement needs and write only the assigned ones (others
	// marked Missing). Engines without batch rows fall back to a full rewrite:
	// read every column, write the whole row.
	needCols := make([]int, len(schema.Columns))
	for i := range needCols {
		needCols[i] = i
	}
	assigned := make(map[string]bool)
	needColsSet := make(map[string]bool)
	for _, item := range s.Sets {
		assigned[item.Column] = true
	}
	if rowsOK {
		need := updateRefColumns(s)
		for _, c := range pkIndex(schema) {
			need[c] = true
		}
		needCols = needCols[:0]
		for i, c := range schema.Columns {
			if need[c.Name] {
				needCols = append(needCols, i)
				needColsSet[c.Name] = true
			}
		}
	}
	tScan := time.Now()
	rows, err := ce.ScanColumns(s.Table, needCols, rng)
	scanDur := time.Since(tScan)
	if err != nil {
		return nil, err
	}
	defer func() {
		if scanDur > 150*time.Millisecond {
			log.Printf("UPDATE-PHASE-SLOW: scan %v", scanDur)
		}
	}()
	updated := make([]Row, 0, len(rows))
	affected := int64(0)
	ctx := make(map[string]Value, len(schema.Columns))
	for _, r := range rows {
		// Reuse one context map across rows; overwrite each column slot
		// before evaluating so no per-row allocation happens.
		for i, c := range schema.Columns {
			ctx[c.Name] = r[i]
		}
		if !rngExact && s.Where != nil {
			v, err := Eval(s.Where, ctx)
			if err != nil {
				return nil, err
			}
			if b, ok := boolOf(v); !ok || !b {
				continue
			}
		}
		if err := evalSetsIntoRow(schema, s.Sets, ctx, r); err != nil {
			return nil, err
		}
		// CH partial-merge: columns that were not read and are not assigned by
		// SET keep their previous value — mark them Missing so the storage
		// inherits the old cell instead of writing a new one. (Read columns
		// that are not SET targets carry their own value already.)
		if rowsOK {
			for i, c := range schema.Columns {
				if !needColsSet[c.Name] && !assigned[c.Name] {
					r[i] = MissingValue
				}
			}
		}
		affected++
		updated = append(updated, r)
	}
	if affected > 0 {
		tWrite := time.Now()
		writeDone := func() {
			if d := time.Since(tWrite); d > 150*time.Millisecond {
				log.Printf("UPDATE-PHASE-SLOW: write %v (scan was %v)", d, scanDur)
			}
		}
		if rowsOK {
			if _, err := br.BatchInsertRows(s.Table, updated); err != nil {
				return nil, err
			}
			writeDone()
		} else if batchOK {
			maps := make([]map[string]Value, 0, len(updated))
			for _, r := range updated {
				m := make(map[string]Value, len(schema.Columns))
				for i, c := range schema.Columns {
					m[c.Name] = r[i]
				}
				maps = append(maps, m)
			}
			if _, err := be.BatchInsert(s.Table, maps); err != nil {
				return nil, err
			}
		} else {
			for _, r := range updated {
				if err := ex.updateRowViaGet(s, schema, r); err != nil {
					return nil, err
				}
			}
		}
	}
	return &Result{Type: "update", Affected: affected}, nil
}

// updateRefColumns collects every column name referenced by an UPDATE's SET
// value expressions and its WHERE predicate (assigned targets excluded).
func updateRefColumns(s *UpdateStmt) map[string]bool {
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
	for _, item := range s.Sets {
		walk(item.Value)
	}
	if s.Where != nil {
		walk(s.Where)
	}
	return refs
}

// updateConstSets returns the column-index→constant map of an UPDATE's SET
// list when every assignment is a plain literal (the bulk-update shape);
// ok=false otherwise. Values go through Eval+Convert for exact parity with
// evalSetsIntoRow, including NULL handling.
func updateConstSets(schema *TableSchema, sets []*SetItem) (map[int]Value, bool) {
	consts := make(map[int]Value, len(sets))
	ctx := map[string]Value{}
	for _, item := range sets {
		if _, ok := item.Value.(*Literal); !ok {
			return nil, false
		}
		v, err := Eval(item.Value, ctx)
		if err != nil {
			return nil, false
		}
		idx := -1
		var colType Type = TypeNull
		for i, c := range schema.Columns {
			if c.Name == item.Column {
				idx, colType = i, c.Type
				break
			}
		}
		if idx < 0 {
			return nil, false
		}
		if colType != TypeNull {
			cv, err := Convert(v, colType)
			if err != nil {
				return nil, false
			}
			v = cv
		}
		consts[idx] = v
	}
	return consts, true
}

// evalSetsIntoRow evaluates the SET assignments of an UPDATE for one row,
// writing the new values into the row slice (index-aligned with the schema).
// ctx is a caller-owned context map already filled with this row's values.
func evalSetsIntoRow(schema *TableSchema, sets []*SetItem, ctx map[string]Value, row Row) error {
	for _, item := range sets {
		v, err := Eval(item.Value, ctx)
		if err != nil {
			return err
		}
		for i, c := range schema.Columns {
			if c.Name == item.Column {
				colType := c.Type
				if colType != TypeNull {
					v, err = Convert(v, colType)
					if err != nil {
						return err
					}
				}
				row[i] = v
				break
			}
		}
	}
	return nil
}

// updateRowViaGet re-applies the SET assignments of an UPDATE to an already
// materialized row through the per-row Update path (used only by engines that
// have neither batch capability).
func (ex *Executor) updateRowViaGet(s *UpdateStmt, schema *TableSchema, row Row) error {
	ctx := make(map[string]Value, len(schema.Columns))
	for i, c := range schema.Columns {
		ctx[c.Name] = row[i]
	}
	set := make(map[string]Value, len(s.Sets))
	for _, item := range s.Sets {
		v, err := Eval(item.Value, ctx)
		if err != nil {
			return err
		}
		set[item.Column] = v
	}
	pk := make([]Value, 0, len(pkIndex(schema)))
	for _, c := range pkIndex(schema) {
		pk = append(pk, ctx[c])
	}
	return ex.Eng.Update(s.Table, pk, set)
}

// execUpdateScan is the generic UPDATE path: a full scan with one Update call
// per matching row.
func (ex *Executor) execUpdateScan(s *UpdateStmt, schema *TableSchema) (*Result, error) {
	rows, err := ex.Eng.Scan(s.Table)
	if err != nil {
		return nil, err
	}
	pkIdx := pkIndex(schema)
	affected := int64(0)
	for _, r := range rows {
		ctx := rowContext(schema, r)
		if s.Where != nil {
			v, err := Eval(s.Where, ctx)
			if err != nil {
				return nil, err
			}
			if b, ok := boolOf(v); !ok || !b {
				continue
			}
		}
		set, err := evalSetsCtx(schema, s.Sets, ctx)
		if err != nil {
			return nil, err
		}
		pk := make([]Value, len(pkIdx))
		for i, c := range pkIdx {
			pk[i] = ctx[c]
		}
		if err := ex.Eng.Update(s.Table, pk, set); err != nil {
			return nil, err
		}
		affected++
	}
	return &Result{Type: "update", Affected: affected}, nil
}

// pkEqualBound reports whether two PK bounds pin the exact same key.
func pkEqualBound(a, b *PKBound) bool {
	if a == nil || b == nil || len(a.Prefix) != len(b.Prefix) {
		return false
	}
	for i := range a.Prefix {
		if c, err := Compare(a.Prefix[i], b.Prefix[i]); err != nil || c != 0 {
			return false
		}
	}
	return true
}

// evalSetsCtx computes the SET assignments of an UPDATE for one row context,
// converting each value to the target column's type.
func evalSetsCtx(schema *TableSchema, sets []*SetItem, ctx map[string]Value) (map[string]Value, error) {
	set := make(map[string]Value, len(sets))
	for _, item := range sets {
		v, err := Eval(item.Value, ctx)
		if err != nil {
			return nil, err
		}
		colType := TypeNull
		for _, c := range schema.Columns {
			if c.Name == item.Column {
				colType = c.Type
				break
			}
		}
		if colType != TypeNull {
			v, err = Convert(v, colType)
			if err != nil {
				return nil, err
			}
		}
		set[item.Column] = v
	}
	return set, nil
}

func (ex *Executor) execDelete(s *DeleteStmt) (*Result, error) {
	schema, err := ex.Eng.GetSchema(s.Table)
	if err != nil {
		return nil, err
	}
	// CSTORE retention: a WHERE that is exactly a range over PK columns is a
	// columnar range delete (markers + cells in one pass), instead of a full
	// scan with per-row deletes.
	if IsColumnarEngine(schema.Engine) {
		if rng, exact := PKRangeFromWhere(schema, s.Where); exact && rng != nil && (rng.Lower != nil || rng.Upper != nil) {
			affected, err := ex.Eng.DeleteRange(s.Table, rng)
			if err != nil {
				return nil, err
			}
			return &Result{Type: "delete", Affected: affected}, nil
		}
	}
	rows, err := ex.Eng.Scan(s.Table)
	if err != nil {
		return nil, err
	}
	pkIdx := pkIndex(schema)
	affected := int64(0)
	for _, r := range rows {
		ctx := rowContext(schema, r)
		if s.Where != nil {
			v, err := Eval(s.Where, ctx)
			if err != nil {
				return nil, err
			}
			if b, ok := boolOf(v); !ok || !b {
				continue
			}
		}
		pk := make([]Value, len(pkIdx))
		for i, c := range pkIdx {
			pk[i] = ctx[c]
		}
		if err := ex.Eng.Delete(s.Table, pk); err != nil {
			return nil, err
		}
		affected++
	}
	return &Result{Type: "delete", Affected: affected}, nil
}

func (ex *Executor) execSelect(s *SelectStmt) (*Result, error) {
	var schema *TableSchema
	if s.From != "" {
		var err error
		schema, err = ex.Eng.GetSchema(s.From)
		if err != nil {
			return nil, err
		}
	}
	// whole-table aggregates over CSTORE: push down before scanning any rows
	hasAgg := false
	for _, it := range s.Items {
		if isAggregate(it.Expr) {
			hasAgg = true
			break
		}
	}
	if hasAgg {
		if r, ok, err := ex.aggregatePushdown(s, schema); err != nil {
			return nil, err
		} else if ok {
			return r, nil
		}
	}
	if len(s.GroupBy) > 0 {
		if r, ok, err := ex.groupedPushdown(s, schema); err != nil {
			return nil, err
		} else if ok {
			return r, nil
		}
	}
	// ORDER BY PK + LIMIT: top-N straight from the PK index, no full scan/sort
	if r, ok, err := ex.topNPushdown(s, schema); err != nil {
		return nil, err
	} else if ok {
		return r, nil
	}
	var rows []Row
	if s.From != "" {
		var err error
		rows, err = ex.rowsFor(s, schema)
		if err != nil {
			return nil, err
		}
	}
	// projection into contexts
	var contexts []map[string]Value
	if schema == nil {
		// SELECT without FROM: single empty context
		contexts = []map[string]Value{{}}
	} else {
		for _, r := range rows {
			contexts = append(contexts, rowContext(schema, r))
		}
	}
	// where filter
	if s.Where != nil {
		filtered := contexts[:0]
		for _, ctx := range contexts {
			v, err := Eval(s.Where, ctx)
			if err != nil {
				return nil, err
			}
			if b, ok := boolOf(v); ok && b {
				filtered = append(filtered, ctx)
			}
		}
		contexts = filtered
	}
	// order by: sort full-width contexts before projection so sort keys that
	// are not selected still resolve; grouped results are not supported here.
	if len(s.OrderBy) > 0 && !hasAgg && len(s.GroupBy) == 0 {
		sortContexts(contexts, s.OrderBy)
	}
	// group by + aggregates
	var out []Row
	if hasAgg {
		groups := map[string][]map[string]Value{}
		for _, ctx := range contexts {
			key := groupKey(s.GroupBy, ctx)
			groups[key] = append(groups[key], ctx)
		}
		for _, group := range groups {
			row := make(Row, len(s.Items))
			for i, it := range s.Items {
				v, err := evalSelectItemAgg(it.Expr, group)
				if err != nil {
					return nil, err
				}
				row[i] = v
			}
			out = append(out, row)
		}
		// a plain aggregate (no GROUP BY) over an empty input still yields one
		// row: count=0, min/max/sum/avg = NULL
		if len(s.GroupBy) == 0 && len(out) == 0 {
			row := make(Row, len(s.Items))
			for i, it := range s.Items {
				v, err := evalSelectItemAgg(it.Expr, nil)
				if err != nil {
					return nil, err
				}
				row[i] = v
			}
			out = append(out, row)
		}
	} else {
		// expand * into per-column projections
		cols := expandSelectItems(s, schema)
		for _, ctx := range contexts {
			row := make(Row, len(cols))
			for i, it := range cols {
				if id, ok := it.Expr.(*Ident); ok && id.Name == "*" {
					row[i] = NullValue
					continue
				}
				v, err := Eval(it.Expr, ctx)
				if err != nil {
					return nil, err
				}
				row[i] = v
			}
			out = append(out, row)
		}
		s.Items = cols
	}
	// distinct
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
	// limit
	if s.HasLimit && int(s.Limit) < len(out) {
		out = out[:s.Limit]
	}
	// column names
	cols := resultColumns(s.Items)
	return &Result{Type: "select", Columns: cols, Rows: out, Affected: int64(len(out))}, nil
}

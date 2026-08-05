package sql

// Row is an ordered slice of values.
type Row []Value

// TableSchema describes a table's columns.
type TableSchema struct {
	Name    string
	Columns []ColumnDef
	PK      []string
}

// Engine is the storage interface used by the executor.
// Implemented by storage.Engine.
type Engine interface {
	CreateTable(schema *TableSchema) error
	DropTable(name string) error
	GetSchema(name string) (*TableSchema, error)
	Insert(table string, row map[string]Value) error
	Scan(table string) ([]Row, error)
	Update(table string, pkValues []Value, set map[string]Value) error
	Delete(table string, pkValues []Value) error
}

// Result is the outcome of executing a statement.
type Result struct {
	Columns  []string // column names (SELECT)
	Rows     []Row    // result rows (SELECT)
	Affected int64    // affected rows for DML
	Type     string   // "select","insert","update","delete","create_table","drop_table","begin","commit","rollback","create_index","drop_index","create_database"
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
		return &Result{Type: "create_index"}, nil
	case *DropIndexStmt:
		return &Result{Type: "drop_index"}, nil
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
	}
	return nil, &ExecError{Msg: "unsupported statement"}
}

type ExecError struct{ Msg string }

func (e *ExecError) Error() string { return e.Msg }

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
	schema := &TableSchema{Name: s.Name, Columns: cols, PK: pk}
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
	affected := int64(0)
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
		set := make(map[string]Value)
		for _, item := range s.Sets {
			v, err := Eval(item.Value, ctx)
			if err != nil {
				return nil, err
			}
			// find column def
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

func (ex *Executor) execDelete(s *DeleteStmt) (*Result, error) {
	schema, err := ex.Eng.GetSchema(s.Table)
	if err != nil {
		return nil, err
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
	var rows []Row
	if s.From != "" {
		var err error
		schema, err = ex.Eng.GetSchema(s.From)
		if err != nil {
			return nil, err
		}
		rows, err = ex.Eng.Scan(s.From)
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
	// group by + aggregates
	var out []Row
	hasAgg := false
	for _, it := range s.Items {
		if isAggregate(it.Expr) {
			hasAgg = true
			break
		}
	}
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
	} else {
		// expand * into per-column projections
		var cols []*SelectItem
		starIdx := -1
		for i, it := range s.Items {
			if id, ok := it.Expr.(*Ident); ok && id.Name == "*" && schema != nil {
				starIdx = i
				continue
			}
			cols = append(cols, it)
		}
		if starIdx >= 0 && schema != nil {
			for _, c := range schema.Columns {
				cols = append(cols, &SelectItem{Expr: &Ident{Name: c.Name}})
			}
		} else if starIdx >= 0 {
			cols = append(cols, &SelectItem{Expr: &Ident{Name: "*"}})
		}
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
	// order by
	if len(s.OrderBy) > 0 && len(s.GroupBy) == 0 {
		sortRows(out, s.OrderBy, schema)
	} else if len(s.OrderBy) > 0 && hasAgg {
		// order by over aggregated result using item aliases/positions not supported simply
	}
	// limit
	if s.HasLimit && int(s.Limit) < len(out) {
		out = out[:s.Limit]
	}
	// column names
	cols := make([]string, len(s.Items))
	for i, it := range s.Items {
		if it.Alias != "" {
			cols[i] = it.Alias
		} else if id, ok := it.Expr.(*Ident); ok {
			cols[i] = id.Name
		} else {
			cols[i] = "col" + string(rune('0'+i))
		}
	}
	return &Result{Type: "select", Columns: cols, Rows: out, Affected: int64(len(out))}, nil
}

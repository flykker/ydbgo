package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Parser struct {
	toks []token
	pos  int
}

// Parse parses a full script into a list of statements.
func Parse(src string) ([]Statement, error) {
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &Parser{toks: toks}
	var stmts []Statement
	for {
		p.skipSemicolons()
		if p.peek().kind == tokEOF {
			break
		}
		st, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, st)
		p.skipSemicolons()
		if p.peek().kind == tokEOF {
			break
		}
	}
	return stmts, nil
}

func (p *Parser) skipSemicolons() {
	for p.peek().kind == tokSemicolon {
		p.pos++
	}
}

func (p *Parser) peek() token { return p.toks[p.pos] }
func (p *Parser) peekAt(n int) token {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return p.toks[len(p.toks)-1]
}
func (p *Parser) advance() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}
func (p *Parser) expect(kind tokKind, what string) (token, error) {
	t := p.peek()
	if t.kind != kind {
		return t, fmt.Errorf("expected %s at pos %d, got %q", what, t.pos, t.text)
	}
	return p.advance(), nil
}
func (p *Parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}
func (p *Parser) acceptKeyword(kw string) bool {
	if p.isKeyword(kw) {
		p.advance()
		return true
	}
	return false
}
func (p *Parser) identLower() string {
	t := p.advance()
	return strings.ToLower(t.text)
}

func (p *Parser) parseStatement() (Statement, error) {
	if p.isKeyword("select") {
		return p.parseSelect()
	}
	if p.isKeyword("insert") {
		return p.parseInsert()
	}
	if p.isKeyword("update") {
		return p.parseUpdate()
	}
	if p.isKeyword("delete") {
		return p.parseDelete()
	}
	if p.isKeyword("create") {
		return p.parseCreate()
	}
	if p.isKeyword("drop") {
		return p.parseDrop()
	}
	if p.isKeyword("begin") {
		p.advance()
		return &BeginStmt{}, nil
	}
	if p.isKeyword("commit") {
		p.advance()
		return &CommitStmt{}, nil
	}
	if p.isKeyword("rollback") {
		p.advance()
		return &RollbackStmt{}, nil
	}
	if p.isKeyword("kv") {
		return p.parseKV()
	}
	return nil, fmt.Errorf("unexpected token %q at pos %d", p.peek().text, p.peek().pos)
}

// parseKV parses the raw byte-KV surface for ENGINE=KV tables:
//
//	KV PUT <table> <key> <value>
//	KV GET <table> <key>
//	KV DELETE <table> <key>
//	KV SCAN <table> [<start> <end>]
//
// keys/values are string literals and are kept as raw bytes in the statement.
func (p *Parser) parseKV() (Statement, error) {
	p.advance() // kv
	switch {
	case p.isKeyword("put"):
		p.advance()
		table := p.identLower()
		key, err := p.stringLiteral()
		if err != nil {
			return nil, err
		}
		value, err := p.stringLiteral()
		if err != nil {
			return nil, err
		}
		return &KVPutStmt{Table: table, Key: key, Value: value}, nil
	case p.isKeyword("get"):
		p.advance()
		table := p.identLower()
		key, err := p.stringLiteral()
		if err != nil {
			return nil, err
		}
		return &KVGetStmt{Table: table, Key: key}, nil
	case p.isKeyword("delete"):
		p.advance()
		table := p.identLower()
		key, err := p.stringLiteral()
		if err != nil {
			return nil, err
		}
		return &KVDeleteStmt{Table: table, Key: key}, nil
	case p.isKeyword("scan"):
		p.advance()
		table := p.identLower()
		st := &KVScanStmt{Table: table}
		if p.peek().kind == tokString {
			start, err := p.stringLiteral()
			if err != nil {
				return nil, err
			}
			st.Start = start
			st.HasStart = true
		}
		if p.peek().kind == tokString {
			end, err := p.stringLiteral()
			if err != nil {
				return nil, err
			}
			st.End = end
			st.HasEnd = true
		}
		return st, nil
	}
	return nil, fmt.Errorf("expected PUT/GET/DELETE/SCAN after KV at pos %d", p.peek().pos)
}

func (p *Parser) stringLiteral() (string, error) {
	t := p.peek()
	if t.kind != tokString {
		return "", fmt.Errorf("expected string literal at pos %d, got %q", t.pos, t.text)
	}
	p.advance()
	return t.text, nil
}

func (p *Parser) parseCreate() (Statement, error) {
	p.advance() // create
	if p.acceptKeyword("database") {
		return &CreateDatabaseStmt{Name: p.identLower()}, nil
	}
	if !p.acceptKeyword("table") {
		return nil, fmt.Errorf("expected TABLE at pos %d", p.peek().pos)
	}
	stmt := &CreateTableStmt{}
	if p.isKeyword("if") {
		p.advance()
		p.expect(tokIdent, "EXISTS")
		stmt.IfNotExists = true
	}
	if p.peek().kind != tokIdent {
		return nil, fmt.Errorf("expected table name at pos %d", p.peek().pos)
	}
	stmt.Name = p.identLower()
	if _, err := p.expect(tokLParen, "("); err != nil {
		return nil, err
	}
	for {
		if p.peek().kind == tokRParen {
			break
		}
		col := ColumnDef{}
		if p.peek().kind != tokIdent {
			return nil, fmt.Errorf("expected column name at pos %d", p.peek().pos)
		}
		col.Name = p.identLower()
		if p.peek().kind == tokIdent {
			col.Type = typeFromKeyword(p.peek().text)
			p.advance()
		}
		// optional: NOT NULL
		if p.isKeyword("not") {
			p.advance()
			p.acceptKeyword("null")
			col.NotNull = true
		}
		// optional: PRIMARY KEY (column-level)
		if p.isKeyword("primary") {
			p.advance()
			p.acceptKeyword("key")
			col.AsPrimary = true
			col.NotNull = true
		}
		// optional: DEFAULT <expr>
		if p.isKeyword("default") {
			p.advance()
			d, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			col.Default = d
		}
		stmt.Columns = append(stmt.Columns, col)
		if p.peek().kind == tokComma {
			p.advance()
			continue
		}
		break
	}
	if _, err := p.expect(tokRParen, ")"); err != nil {
		return nil, err
	}
	// optional table-level: PRIMARY KEY(...)
	if p.isKeyword("primary") {
		p.advance()
		p.acceptKeyword("key")
		if _, err := p.expect(tokLParen, "("); err != nil {
			return nil, err
		}
		for {
			if p.peek().kind != tokIdent {
				return nil, fmt.Errorf("expected pk column at pos %d", p.peek().pos)
			}
			stmt.PK = append(stmt.PK, p.identLower())
			if p.peek().kind == tokComma {
				p.advance()
				continue
			}
			break
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
	}
	// optional: ENGINE=<TABLE|KV|CSTORE>
	if p.acceptKeyword("engine") {
		if _, err := p.expect(tokOp, "="); err != nil {
			return nil, fmt.Errorf("expected = after ENGINE at pos %d", p.peek().pos)
		}
		if p.peek().kind != tokIdent {
			return nil, fmt.Errorf("expected engine name (TABLE|KV|CSTORE) at pos %d", p.peek().pos)
		}
		stmt.Engine = p.identLower()
		switch stmt.Engine {
		case "table", "kv", "cstore":
		default:
			return nil, fmt.Errorf("unknown ENGINE %q (want TABLE, KV or CSTORE)", stmt.Engine)
		}
	}
	// optional: RETENTION = '<duration>' — auto-delete rows older than the window
	if p.acceptKeyword("retention") {
		if _, err := p.expect(tokOp, "="); err != nil {
			return nil, fmt.Errorf("expected = after RETENTION at pos %d", p.peek().pos)
		}
		if p.peek().kind != tokString {
			return nil, fmt.Errorf("expected RETENTION duration string (e.g. '24h', '7d') at pos %d", p.peek().pos)
		}
		d, err := parseRetention(p.advance().text)
		if err != nil {
			return nil, fmt.Errorf("bad RETENTION at pos %d: %v", p.peek().pos, err)
		}
		stmt.Retention = d
	}
	return stmt, nil
}

// parseRetention parses a retention window like "24h", "30m", "7d" or "3600s".
func parseRetention(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return 0, errors.New("too short")
	}
	if s[len(s)-1] == 'd' {
		n, err := strconv.ParseInt(strings.TrimSpace(s[:len(s)-1]), 10, 64)
		if err != nil || n <= 0 {
			return 0, errors.New("invalid day count")
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, errors.New("invalid duration")
	}
	return d, nil
}

func typeFromKeyword(kw string) Type {
	switch strings.ToLower(kw) {
	case "int", "int64", "integer":
		return TypeInt
	case "float", "double", "real":
		return TypeFloat
	case "string", "text", "varchar":
		return TypeString
	case "bool", "boolean":
		return TypeBool
	case "timestamp":
		return TypeTimestamp
	}
	return TypeNull
}

func (p *Parser) parseDrop() (Statement, error) {
	p.advance() // drop
	if p.acceptKeyword("table") {
		stmt := &DropTableStmt{}
		if p.isKeyword("if") {
			p.advance()
			p.expect(tokIdent, "EXISTS")
			stmt.IfExists = true
		}
		if p.peek().kind != tokIdent {
			return nil, fmt.Errorf("expected table name at pos %d", p.peek().pos)
		}
		stmt.Name = p.identLower()
		return stmt, nil
	}
	if p.acceptKeyword("index") {
		stmt := &DropIndexStmt{}
		if p.isKeyword("if") {
			p.advance()
			p.expect(tokIdent, "EXISTS")
			stmt.IfExists = true
		}
		if p.peek().kind != tokIdent {
			return nil, fmt.Errorf("expected index name at pos %d", p.peek().pos)
		}
		stmt.Name = p.identLower()
		if p.peek().kind == tokIdent {
			stmt.Table = p.identLower()
		}
		return stmt, nil
	}
	return nil, fmt.Errorf("expected TABLE or INDEX at pos %d", p.peek().pos)
}

func (p *Parser) parseInsert() (Statement, error) {
	p.advance() // insert
	p.acceptKeyword("into")
	stmt := &InsertStmt{}
	if p.peek().kind != tokIdent {
		return nil, fmt.Errorf("expected table name at pos %d", p.peek().pos)
	}
	stmt.Table = p.identLower()
	if p.peek().kind == tokLParen {
		p.advance()
		for {
			if p.peek().kind != tokIdent {
				return nil, fmt.Errorf("expected column name at pos %d", p.peek().pos)
			}
			stmt.Columns = append(stmt.Columns, p.identLower())
			if p.peek().kind == tokComma {
				p.advance()
				continue
			}
			break
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
	}
	p.acceptKeyword("values")
	for {
		if _, err := p.expect(tokLParen, "("); err != nil {
			return nil, err
		}
		var row []Expr
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			row = append(row, e)
			if p.peek().kind == tokComma {
				p.advance()
				continue
			}
			break
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
		stmt.Rows = append(stmt.Rows, row)
		if p.peek().kind == tokComma {
			p.advance()
			continue
		}
		break
	}
	return stmt, nil
}

func (p *Parser) parseUpdate() (Statement, error) {
	p.advance() // update
	stmt := &UpdateStmt{}
	if p.peek().kind != tokIdent {
		return nil, fmt.Errorf("expected table name at pos %d", p.peek().pos)
	}
	stmt.Table = p.identLower()
	p.acceptKeyword("set")
	for {
		if p.peek().kind != tokIdent {
			return nil, fmt.Errorf("expected column name at pos %d", p.peek().pos)
		}
		col := p.identLower()
		if _, err := p.expect(tokOp, "="); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Sets = append(stmt.Sets, &SetItem{Column: col, Value: val})
		if p.peek().kind == tokComma {
			p.advance()
			continue
		}
		break
	}
	if p.acceptKeyword("where") {
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}
	return stmt, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	p.advance() // delete
	p.acceptKeyword("from")
	stmt := &DeleteStmt{}
	if p.peek().kind != tokIdent {
		return nil, fmt.Errorf("expected table name at pos %d", p.peek().pos)
	}
	stmt.Table = p.identLower()
	if p.acceptKeyword("where") {
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}
	return stmt, nil
}

func (p *Parser) parseSelect() (Statement, error) {
	p.advance() // select
	stmt := &SelectStmt{}
	if p.acceptKeyword("distinct") {
		stmt.Distinct = true
	}
	// select items
	for {
		item := &SelectItem{}
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item.Expr = e
		if p.acceptKeyword("as") {
			if p.peek().kind != tokIdent {
				return nil, fmt.Errorf("expected alias at pos %d", p.peek().pos)
			}
			item.Alias = p.identLower()
		}
		stmt.Items = append(stmt.Items, item)
		if p.peek().kind == tokComma {
			p.advance()
			continue
		}
		break
	}
	if p.acceptKeyword("from") {
		if p.peek().kind != tokIdent {
			return nil, fmt.Errorf("expected table name at pos %d", p.peek().pos)
		}
		stmt.From = p.identLower()
	}
	if p.acceptKeyword("where") {
		w, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}
	if p.acceptKeyword("group") {
		p.acceptKeyword("by")
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, e)
			if p.peek().kind == tokComma {
				p.advance()
				continue
			}
			break
		}
	}
	if p.acceptKeyword("having") {
		h, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = &BinaryOp{Op: "AND", Left: stmt.Where, Right: h}
	}
	if p.acceptKeyword("order") {
		p.acceptKeyword("by")
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			item := &OrderItem{Expr: e}
			if p.acceptKeyword("desc") {
				item.Desc = true
			} else {
				p.acceptKeyword("asc")
			}
			stmt.OrderBy = append(stmt.OrderBy, item)
			if p.peek().kind == tokComma {
				p.advance()
				continue
			}
			break
		}
	}
	if p.acceptKeyword("limit") {
		t := p.advance()
		if t.kind != tokNumber {
			return nil, fmt.Errorf("expected number after LIMIT at pos %d", t.pos)
		}
		stmt.Limit = t.intVal
		stmt.HasLimit = true
	}
	return stmt, nil
}

// parseExpr handles precedence climbing.
func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("or") {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: "OR", Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("and") {
		p.advance()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: "AND", Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tokOp && (t.text == "=" || t.text == "==" || t.text == "<>" || t.text == "!=" || t.text == "<" || t.text == "<=" || t.text == ">" || t.text == ">=") {
			p.advance()
			right, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			op := t.text
			if op == "==" {
				op = "="
			}
			if op == "!=" {
				op = "<>"
			}
			left = &BinaryOp{Op: op, Left: left, Right: right}
			continue
		}
		if p.isKeyword("not") && p.peekAt(1).kind == tokIdent && strings.EqualFold(p.peekAt(1).text, "like") {
			p.advance() // not
			p.advance() // like
			right, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			left = &BinaryOp{Op: "NOT LIKE", Left: left, Right: right}
			continue
		}
		if p.isKeyword("like") {
			p.advance()
			right, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			left = &BinaryOp{Op: "LIKE", Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *Parser) parseAdditive() (Expr, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tokOp && (t.text == "+" || t.text == "-") {
			p.advance()
			right, err := p.parseMultiplicative()
			if err != nil {
				return nil, err
			}
			left = &BinaryOp{Op: t.text, Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *Parser) parseMultiplicative() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if (t.kind == tokOp && t.text == "*") || t.kind == tokStar {
			p.advance()
			right, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			left = &BinaryOp{Op: "*", Left: left, Right: right}
			continue
		}
		if t.kind == tokOp && t.text == "/" {
			p.advance()
			right, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			left = &BinaryOp{Op: "/", Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *Parser) parseUnary() (Expr, error) {
	t := p.peek()
	if t.kind == tokOp && (t.text == "-" || t.text == "+") {
		p.advance()
		e, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Op: t.text, Expr: e}, nil
	}
	if p.isKeyword("not") {
		p.advance()
		e, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Op: "NOT", Expr: e}, nil
	}
	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Expr, error) {
	t := p.peek()
	switch t.kind {
	case tokNumber:
		p.advance()
		if t.isFloat {
			return &Literal{Type: TypeFloat, Float: t.fltVal}, nil
		}
		return &Literal{Type: TypeInt, Int: t.intVal}, nil
	case tokString:
		p.advance()
		return &Literal{Type: TypeString, Str: t.text}, nil
	case tokStar:
		p.advance()
		return &Ident{Name: "*"}, nil
	case tokLParen:
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen, ")"); err != nil {
			return nil, err
		}
		return e, nil
	case tokIdent:
		if strings.EqualFold(t.text, "null") {
			p.advance()
			return &Literal{Type: TypeNull}, nil
		}
		if strings.EqualFold(t.text, "true") || strings.EqualFold(t.text, "false") {
			p.advance()
			return &Literal{Type: TypeBool, Bool: strings.EqualFold(t.text, "true")}, nil
		}
		if strings.EqualFold(t.text, "cast") {
			p.advance()
			if _, err := p.expect(tokLParen, "("); err != nil {
				return nil, err
			}
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			p.acceptKeyword("as")
			if p.peek().kind != tokIdent {
				return nil, fmt.Errorf("expected type after AS at pos %d", p.peek().pos)
			}
			ty := typeFromKeyword(p.advance().text)
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			return &CastExpr{Expr: e, Type: ty}, nil
		}
		// function call?
		if p.peekAt(1).kind == tokLParen {
			p.advance()
			p.advance()
			var args []Expr
			if p.peek().kind != tokRParen {
				for {
					a, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					args = append(args, a)
					if p.peek().kind == tokComma {
						p.advance()
						continue
					}
					break
				}
			}
			if _, err := p.expect(tokRParen, ")"); err != nil {
				return nil, err
			}
			return &Call{Name: strings.ToLower(t.text), Args: args}, nil
		}
		p.advance()
		return &Ident{Name: strings.ToLower(t.text)}, nil
	default:
		return nil, fmt.Errorf("unexpected token %q at pos %d", t.text, t.pos)
	}
}

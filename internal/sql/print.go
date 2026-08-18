package sql

import (
	"fmt"
	"strings"
)

// StatementString renders a statement back to SQL text.
// Used to ship statements through Raft deterministically.
func StatementString(st Statement) string {
	switch n := st.(type) {
	case *CreateTableStmt:
		return createTableString(n)
	case *DropTableStmt:
		s := "drop table "
		if n.IfExists {
			s += "if exists "
		}
		return s + n.Name
	case *CreateIndexStmt:
		s := "create index "
		if n.IfNotExists {
			s += "if not exists "
		}
		return fmt.Sprintf("%s%s on %s (%s)", s, n.Name, n.Table, strings.Join(n.Columns, ", "))
	case *DropIndexStmt:
		s := "drop index "
		if n.IfExists {
			s += "if exists "
		}
		return s + n.Name
	case *InsertStmt:
		var sb strings.Builder
		sb.WriteString("insert into ")
		sb.WriteString(n.Table)
		if len(n.Columns) > 0 {
			sb.WriteString(" (" + strings.Join(n.Columns, ", ") + ")")
		}
		sb.WriteString(" values ")
		for i, row := range n.Rows {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			parts := make([]string, len(row))
			for j, e := range row {
				parts[j] = ExprString(e)
			}
			sb.WriteString(strings.Join(parts, ", "))
			sb.WriteString(")")
		}
		return sb.String()
	case *UpdateStmt:
		var sb strings.Builder
		sb.WriteString("update ")
		sb.WriteString(n.Table)
		sb.WriteString(" set ")
		parts := make([]string, len(n.Sets))
		for i, s := range n.Sets {
			parts[i] = s.Column + " = " + ExprString(s.Value)
		}
		sb.WriteString(strings.Join(parts, ", "))
		if n.Where != nil {
			sb.WriteString(" where " + ExprString(n.Where))
		}
		return sb.String()
	case *DeleteStmt:
		s := "delete from " + n.Table
		if n.Where != nil {
			s += " where " + ExprString(n.Where)
		}
		return s
	case *BeginStmt:
		return "begin"
	case *CommitStmt:
		return "commit"
	case *RollbackStmt:
		return "rollback"
	case *CreateDatabaseStmt:
		return "create database " + n.Name
	case *KVPutStmt:
		return "kv put " + n.Table + " " + sqlStr(n.Key) + " " + sqlStr(n.Value)
	case *KVGetStmt:
		return "kv get " + n.Table + " " + sqlStr(n.Key)
	case *KVDeleteStmt:
		return "kv delete " + n.Table + " " + sqlStr(n.Key)
	case *KVScanStmt:
		s := "kv scan " + n.Table
		if n.HasStart {
			s += " " + sqlStr(n.Start)
		}
		if n.HasEnd {
			s += " " + sqlStr(n.End)
		}
		return s
	}
	return ""
}

// sqlStr renders a raw byte string as a SQL string literal.
func sqlStr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func createTableString(n *CreateTableStmt) string {
	var sb strings.Builder
	sb.WriteString("create table ")
	sb.WriteString(n.Name)
	sb.WriteString(" (")
	parts := make([]string, 0, len(n.Columns))
	for _, c := range n.Columns {
		p := c.Name + " " + c.Type.String()
		if c.NotNull {
			p += " not null"
		}
		if c.AsPrimary {
			p += " primary key"
		}
		parts = append(parts, p)
	}
	sb.WriteString(strings.Join(parts, ", "))
	sb.WriteString(")")
	if len(n.PK) > 0 {
		sb.WriteString(" primary key (" + strings.Join(n.PK, ", ") + ")")
	}
	if n.Engine != "" {
		sb.WriteString(" engine=" + strings.ToUpper(n.Engine))
	}
	if n.Retention > 0 {
		sb.WriteString(" retention='" + n.Retention.String() + "'")
	}
	return sb.String()
}

// ExprString renders an expression as SQL text.
func ExprString(e Expr) string {
	switch n := e.(type) {
	case *Literal:
		switch n.Type {
		case TypeNull:
			return "null"
		case TypeInt:
			return fmt.Sprintf("%d", n.Int)
		case TypeFloat:
			return fmt.Sprintf("%g", n.Float)
		case TypeString:
			return "'" + strings.ReplaceAll(n.Str, "'", "''") + "'"
		case TypeBool:
			if n.Bool {
				return "true"
			}
			return "false"
		case TypeTimestamp:
			return "'" + strings.ReplaceAll(n.Str, "'", "''") + "'"
		}
	case *Ident:
		return n.Name
	case *UnaryOp:
		return "(" + n.Op + " " + ExprString(n.Expr) + ")"
	case *CastExpr:
		return "cast(" + ExprString(n.Expr) + " as " + n.Type.String() + ")"
	case *BinaryOp:
		return "(" + ExprString(n.Left) + " " + n.Op + " " + ExprString(n.Right) + ")"
	case *Call:
		args := make([]string, len(n.Args))
		for i, a := range n.Args {
			args[i] = ExprString(a)
		}
		return n.Name + "(" + strings.Join(args, ", ") + ")"
	}
	return ""
}

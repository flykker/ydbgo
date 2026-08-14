package sql

import (
	"fmt"
	"sort"
	"strings"
)

// notFoundError is implemented by storage to signal "table not found".
type notFoundError interface {
	NotFound() bool
}

func isNotFound(err error) bool {
	nf, ok := err.(notFoundError)
	return ok && nf.NotFound()
}

func zeroValue(c ColumnDef) Value {
	switch c.Type {
	case TypeInt:
		return IntValue(0)
	case TypeFloat:
		return FloatValue(0)
	case TypeString:
		return StrValue("")
	case TypeBool:
		return BoolValue(false)
	case TypeTimestamp:
		return NullValue
	}
	return NullValue
}

func pkIndex(s *TableSchema) []string {
	if len(s.PK) > 0 {
		return s.PK
	}
	var pk []string
	for _, c := range s.Columns {
		if c.AsPrimary {
			pk = append(pk, c.Name)
		}
	}
	return pk
}

// rowContext maps column names to values for a row.
func rowContext(s *TableSchema, row Row) map[string]Value {
	ctx := make(map[string]Value, len(row))
	for i, c := range s.Columns {
		if i < len(row) {
			ctx[c.Name] = row[i]
		}
	}
	return ctx
}

func isAggregate(e Expr) bool {
	switch n := e.(type) {
	case *Call:
		switch strings.ToLower(n.Name) {
		case "count", "sum", "min", "max", "avg":
			return true
		}
	case *BinaryOp:
		return isAggregate(n.Left) || isAggregate(n.Right)
	case *CastExpr:
		return isAggregate(n.Expr)
	}
	return false
}

func groupKey(exprs []Expr, ctx map[string]Value) string {
	if len(exprs) == 0 {
		return ""
	}
	var parts []string
	for _, e := range exprs {
		v, err := Eval(e, ctx)
		if err != nil {
			parts = append(parts, "<err>")
			continue
		}
		parts = append(parts, v.Type.String()+":"+v.String())
	}
	return strings.Join(parts, "\x00")
}

func rowKeyString(r Row) string {
	var parts []string
	for _, v := range r {
		parts = append(parts, v.Type.String()+":"+v.String())
	}
	return strings.Join(parts, "\x00")
}

// sortContexts sorts full-width row contexts by the ORDER BY expressions. It is
// applied before projection because projected rows carry only the selected
// columns and cannot be compared by unselected sort keys.
func sortContexts(contexts []map[string]Value, order []*OrderItem) {
	sort.SliceStable(contexts, func(i, j int) bool {
		for _, o := range order {
			vi, err1 := Eval(o.Expr, contexts[i])
			vj, err2 := Eval(o.Expr, contexts[j])
			if err1 != nil || err2 != nil {
				continue
			}
			c, err := Compare(vi, vj)
			if err != nil {
				continue
			}
			if c == 0 {
				continue
			}
			if o.Desc {
				return c > 0
			}
			return c < 0
		}
		return false
	})
}

// evalSelectItemAgg evaluates aggregate expressions.
func evalSelectItemAgg(e Expr, rows []map[string]Value) (Value, error) {
	switch n := e.(type) {
	case *Call:
		return evalAggregateCall(n, rows)
	case *Ident:
		// plain column in grouped query: take first value
		if len(rows) == 0 {
			return NullValue, nil
		}
		return rows[0][n.Name], nil
	case *Literal:
		return Eval(e, nil)
	case *BinaryOp:
		l, err := evalSelectItemAgg(n.Left, rows)
		if err != nil {
			return NullValue, err
		}
		r, err := evalSelectItemAgg(n.Right, rows)
		if err != nil {
			return NullValue, err
		}
		return evalBinaryValues(n.Op, l, r)
	case *CastExpr:
		v, err := evalSelectItemAgg(n.Expr, rows)
		if err != nil {
			return NullValue, err
		}
		return Convert(v, n.Type)
	case *UnaryOp:
		v, err := evalSelectItemAgg(n.Expr, rows)
		if err != nil {
			return NullValue, err
		}
		if n.Op == "-" {
			if v.Type == TypeInt {
				return IntValue(-v.Int), nil
			}
			if f, ok := v.AsFloat(); ok {
				return FloatValue(-f), nil
			}
		}
		return v, nil
	}
	return NullValue, fmt.Errorf("cannot evaluate aggregate %T", e)
}

func evalAggregateCall(n *Call, rows []map[string]Value) (Value, error) {
	name := strings.ToLower(n.Name)
	switch name {
	case "count":
		if len(n.Args) == 1 {
			// count(*) or count(col)
			if id, ok := n.Args[0].(*Ident); ok && id.Name == "*" {
				return IntValue(int64(len(rows))), nil
			}
			cnt := int64(0)
			for _, ctx := range rows {
				v, err := Eval(n.Args[0], ctx)
				if err != nil {
					return NullValue, err
				}
				if !v.Null {
					cnt++
				}
			}
			return IntValue(cnt), nil
		}
		return IntValue(int64(len(rows))), nil
	case "sum":
		var total float64
		isInt := true
		any := false
		for _, ctx := range rows {
			v, err := Eval(n.Args[0], ctx)
			if err != nil {
				return NullValue, err
			}
			if v.Null {
				continue
			}
			if v.Type != TypeInt {
				isInt = false
			}
			f, ok := v.AsFloat()
			if !ok {
				return NullValue, fmt.Errorf("sum requires numeric")
			}
			total += f
			any = true
		}
		if !any {
			return NullValue, nil
		}
		if isInt {
			return IntValue(int64(total)), nil
		}
		return FloatValue(total), nil
	case "min", "max":
		var best Value
		bestSet := false
		for _, ctx := range rows {
			v, err := Eval(n.Args[0], ctx)
			if err != nil {
				return NullValue, err
			}
			if v.Null {
				continue
			}
			if !bestSet {
				best, bestSet = v, true
				continue
			}
			c, err := Compare(v, best)
			if err != nil {
				return NullValue, err
			}
			if (name == "min" && c < 0) || (name == "max" && c > 0) {
				best = v
			}
		}
		if !bestSet {
			return NullValue, nil
		}
		return best, nil
	case "avg":
		var total float64
		cnt := 0
		for _, ctx := range rows {
			v, err := Eval(n.Args[0], ctx)
			if err != nil {
				return NullValue, err
			}
			if v.Null {
				continue
			}
			f, ok := v.AsFloat()
			if !ok {
				return NullValue, fmt.Errorf("avg requires numeric")
			}
			total += f
			cnt++
		}
		if cnt == 0 {
			return NullValue, nil
		}
		return FloatValue(total / float64(cnt)), nil
	}
	return evalCall(n, rows[0])
}

func evalBinaryValues(op string, l, r Value) (Value, error) {
	return evalBinaryOpValues(op, l, r)
}

// helper: re-evaluate binary using values via temp context is awkward;
// instead we inline a simpler path:
func evalBinaryOpValues(op string, l, r Value) (Value, error) {
	switch strings.ToUpper(op) {
	case "AND":
		lb, lok := boolOf(l)
		rb, rok := boolOf(r)
		if lok && !lb {
			return BoolValue(false), nil
		}
		if rok && !rb {
			return BoolValue(false), nil
		}
		if lok && rok {
			return BoolValue(lb && rb), nil
		}
		return NullValue, nil
	case "OR":
		lb, lok := boolOf(l)
		rb, rok := boolOf(r)
		if lok && lb {
			return BoolValue(true), nil
		}
		if rok && rb {
			return BoolValue(true), nil
		}
		if lok && rok {
			return BoolValue(false), nil
		}
		return NullValue, nil
	case "+", "-", "*", "/":
		if l.Type == TypeInt && r.Type == TypeInt {
			switch op {
			case "+":
				return IntValue(l.Int + r.Int), nil
			case "-":
				return IntValue(l.Int - r.Int), nil
			case "*":
				return IntValue(l.Int * r.Int), nil
			case "/":
				if r.Int == 0 {
					return NullValue, fmt.Errorf("division by zero")
				}
				return IntValue(l.Int / r.Int), nil
			}
		}
		lf, lok := l.AsFloat()
		rf, rok := r.AsFloat()
		if !lok || !rok {
			return NullValue, fmt.Errorf("arithmetic requires numbers")
		}
		switch op {
		case "+":
			return FloatValue(lf + rf), nil
		case "-":
			return FloatValue(lf - rf), nil
		case "*":
			return FloatValue(lf * rf), nil
		case "/":
			if rf == 0 {
				return NullValue, fmt.Errorf("division by zero")
			}
			return FloatValue(lf / rf), nil
		}
	}
	return NullValue, fmt.Errorf("unsupported operator %q", op)
}

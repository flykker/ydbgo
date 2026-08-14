package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Value is a dynamic SQL value.
type Value struct {
	Type Type
	Null bool
	Int  int64
	Flt  float64
	Str  string
	Bool bool
	Tm   time.Time
}

var NullValue = Value{Type: TypeNull, Null: true}

func IntValue(v int64) Value     { return Value{Type: TypeInt, Int: v} }
func FloatValue(v float64) Value { return Value{Type: TypeFloat, Flt: v} }
func StrValue(v string) Value    { return Value{Type: TypeString, Str: v} }
func BoolValue(v bool) Value     { return Value{Type: TypeBool, Bool: v} }
func TimestampValue(t time.Time) Value {
	return Value{Type: TypeTimestamp, Tm: t}
}

func (v Value) String() string {
	switch v.Type {
	case TypeNull:
		return "NULL"
	case TypeInt:
		return strconv.FormatInt(v.Int, 10)
	case TypeFloat:
		return strconv.FormatFloat(v.Flt, 'g', -1, 64)
	case TypeString:
		return v.Str
	case TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case TypeTimestamp:
		return v.Tm.Format(time.RFC3339Nano)
	}
	return ""
}

// AsFloat converts to float64 if possible.
func (v Value) AsFloat() (float64, bool) {
	switch v.Type {
	case TypeInt:
		return float64(v.Int), true
	case TypeFloat:
		return v.Flt, true
	}
	return 0, false
}

// Compare returns -1,0,1 comparing two values. Handles numeric promotion.
func Compare(a, b Value) (int, error) {
	if a.Null || b.Null {
		if a.Null && b.Null {
			return 0, nil
		}
		if a.Null {
			return -1, nil
		}
		return 1, nil
	}
	// numeric promotion
	if a.Type == TypeInt && b.Type == TypeInt {
		switch {
		case a.Int < b.Int:
			return -1, nil
		case a.Int > b.Int:
			return 1, nil
		}
		return 0, nil
	}
	if (a.Type == TypeInt || a.Type == TypeFloat) && (b.Type == TypeInt || b.Type == TypeFloat) {
		af, _ := a.AsFloat()
		bf, _ := b.AsFloat()
		switch {
		case af < bf:
			return -1, nil
		case af > bf:
			return 1, nil
		}
		return 0, nil
	}
	switch {
	case a.Type == TypeString && b.Type == TypeString:
		return strings.Compare(a.Str, b.Str), nil
	case a.Type == TypeBool && b.Type == TypeBool:
		if a.Bool == b.Bool {
			return 0, nil
		}
		if !a.Bool {
			return -1, nil
		}
		return 1, nil
	case a.Type == TypeTimestamp && b.Type == TypeTimestamp:
		switch {
		case a.Tm.Before(b.Tm):
			return -1, nil
		case a.Tm.After(b.Tm):
			return 1, nil
		}
		return 0, nil
	case a.Type == TypeString && b.Type == TypeTimestamp:
		if t, err := time.Parse(time.RFC3339Nano, a.Str); err == nil {
			return compareTimes(b.Tm, t)
		}
	case a.Type == TypeTimestamp && b.Type == TypeString:
		if t, err := time.Parse(time.RFC3339Nano, b.Str); err == nil {
			return compareTimes(a.Tm, t)
		}
	case a.Type == TypeTimestamp && b.Type == TypeInt:
		return compareTimes(a.Tm, time.Unix(b.Int, 0))
	case a.Type == TypeInt && b.Type == TypeTimestamp:
		return compareTimes(time.Unix(a.Int, 0), b.Tm)
	}
	// cross-type string compare by display value
	return strings.Compare(a.String(), b.String()), nil
}

func compareTimes(a, b time.Time) (int, error) {
	switch {
	case a.Before(b):
		return -1, nil
	case a.After(b):
		return 1, nil
	}
	return 0, nil
}

// Convert coerces a value to a target type.
func Convert(v Value, t Type) (Value, error) {
	if v.Null {
		return NullValue, nil
	}
	switch t {
	case TypeInt:
		switch v.Type {
		case TypeInt:
			return v, nil
		case TypeFloat:
			return IntValue(int64(v.Flt)), nil
		case TypeString:
			i, err := strconv.ParseInt(strings.TrimSpace(v.Str), 10, 64)
			if err != nil {
				return v, fmt.Errorf("cannot convert %q to int64", v.Str)
			}
			return IntValue(i), nil
		case TypeBool:
			if v.Bool {
				return IntValue(1), nil
			}
			return IntValue(0), nil
		}
	case TypeFloat:
		switch v.Type {
		case TypeFloat:
			return v, nil
		case TypeInt:
			return FloatValue(float64(v.Int)), nil
		case TypeString:
			f, err := strconv.ParseFloat(strings.TrimSpace(v.Str), 64)
			if err != nil {
				return v, fmt.Errorf("cannot convert %q to float64", v.Str)
			}
			return FloatValue(f), nil
		}
	case TypeString:
		return StrValue(v.String()), nil
	case TypeBool:
		switch v.Type {
		case TypeBool:
			return v, nil
		case TypeInt:
			return BoolValue(v.Int != 0), nil
		case TypeString:
			switch strings.ToLower(strings.TrimSpace(v.Str)) {
			case "true", "1":
				return BoolValue(true), nil
			case "false", "0":
				return BoolValue(false), nil
			}
			return v, fmt.Errorf("cannot convert %q to bool", v.Str)
		}
	case TypeTimestamp:
		switch v.Type {
		case TypeTimestamp:
			return v, nil
		case TypeInt:
			return TimestampValue(time.Unix(v.Int, 0)), nil
		case TypeString:
			t, err := time.Parse(time.RFC3339Nano, v.Str)
			if err != nil {
				return v, fmt.Errorf("cannot convert %q to timestamp", v.Str)
			}
			return TimestampValue(t), nil
		}
	}
	return v, fmt.Errorf("cannot convert %s to %s", v.Type, t)
}

// Eval evaluates an expression against a row context.
// ctx maps lowercased column names to values.
func Eval(e Expr, ctx map[string]Value) (Value, error) {
	switch n := e.(type) {
	case *Literal:
		if n.Type == TypeNull {
			return NullValue, nil
		}
		switch n.Type {
		case TypeInt:
			return IntValue(n.Int), nil
		case TypeFloat:
			return FloatValue(n.Float), nil
		case TypeString:
			return StrValue(n.Str), nil
		case TypeBool:
			return BoolValue(n.Bool), nil
		}
		return NullValue, nil
	case *Ident:
		if n.Name == "*" {
			return NullValue, nil
		}
		if v, ok := ctx[n.Name]; ok {
			return v, nil
		}
		return NullValue, fmt.Errorf("unknown column %q", n.Name)
	case *UnaryOp:
		v, err := Eval(n.Expr, ctx)
		if err != nil {
			return NullValue, err
		}
		switch strings.ToUpper(n.Op) {
		case "NOT":
			if v.Type == TypeBool {
				return BoolValue(!v.Bool), nil
			}
			return NullValue, fmt.Errorf("NOT requires bool, got %s", v.Type)
		case "-":
			if v.Type == TypeInt {
				return IntValue(-v.Int), nil
			}
			if f, ok := v.AsFloat(); ok {
				return FloatValue(-f), nil
			}
			return NullValue, fmt.Errorf("cannot negate %s", v.Type)
		case "+":
			return v, nil
		}
		return NullValue, fmt.Errorf("unknown unary op %q", n.Op)
	case *CastExpr:
		v, err := Eval(n.Expr, ctx)
		if err != nil {
			return NullValue, err
		}
		return Convert(v, n.Type)
	case *BinaryOp:
		return evalBinary(n, ctx)
	case *Call:
		return evalCall(n, ctx)
	}
	return NullValue, fmt.Errorf("cannot evaluate expression %T", e)
}

func evalBinary(n *BinaryOp, ctx map[string]Value) (Value, error) {
	op := strings.ToUpper(n.Op)
	if op == "AND" || op == "OR" {
		l, err := Eval(n.Left, ctx)
		if err != nil {
			return NullValue, err
		}
		r, err := Eval(n.Right, ctx)
		if err != nil {
			return NullValue, err
		}
		lb, lok := boolOf(l)
		rb, rok := boolOf(r)
		if op == "AND" {
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
		}
		// OR
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
	}
	if op == "LIKE" || op == "NOT LIKE" {
		l, err := Eval(n.Left, ctx)
		if err != nil {
			return NullValue, err
		}
		r, err := Eval(n.Right, ctx)
		if err != nil {
			return NullValue, err
		}
		if l.Null || r.Null {
			return NullValue, nil
		}
		m := likeMatch(l.Str, r.Str)
		if op == "NOT LIKE" {
			return BoolValue(!m), nil
		}
		return BoolValue(m), nil
	}
	l, err := Eval(n.Left, ctx)
	if err != nil {
		return NullValue, err
	}
	r, err := Eval(n.Right, ctx)
	if err != nil {
		return NullValue, err
	}
	switch op {
	case "=", "<>":
		if l.Null || r.Null {
			return NullValue, nil
		}
		c, err := Compare(l, r)
		if err != nil {
			return NullValue, err
		}
		if op == "=" {
			return BoolValue(c == 0), nil
		}
		return BoolValue(c != 0), nil
	case "<", "<=", ">", ">=":
		if l.Null || r.Null {
			return NullValue, nil
		}
		c, err := Compare(l, r)
		if err != nil {
			return NullValue, err
		}
		switch op {
		case "<":
			return BoolValue(c < 0), nil
		case "<=":
			return BoolValue(c <= 0), nil
		case ">":
			return BoolValue(c > 0), nil
		case ">=":
			return BoolValue(c >= 0), nil
		}
	case "+", "-", "*", "/":
		if l.Null || r.Null {
			return NullValue, nil
		}
		if l.Type == TypeInt && r.Type == TypeInt {
			// integer arithmetic
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
			return NullValue, fmt.Errorf("arithmetic requires numbers, got %s and %s", l.Type, r.Type)
		}
		isFloat := l.Type == TypeFloat || r.Type == TypeFloat
		var res float64
		switch op {
		case "+":
			res = lf + rf
		case "-":
			res = lf - rf
		case "*":
			res = lf * rf
		case "/":
			if rf == 0 {
				return NullValue, fmt.Errorf("division by zero")
			}
			res = lf / rf
		}
		if isFloat {
			return FloatValue(res), nil
		}
		return IntValue(int64(math.Round(res))), nil
	}
	return NullValue, fmt.Errorf("unsupported operator %q", n.Op)
}

func boolOf(v Value) (bool, bool) {
	if v.Null {
		return false, false
	}
	switch v.Type {
	case TypeBool:
		return v.Bool, true
	case TypeInt:
		return v.Int != 0, true
	}
	return false, false
}

func evalCall(n *Call, ctx map[string]Value) (Value, error) {
	name := strings.ToLower(n.Name)
	switch name {
	case "abs":
		v, err := Eval(n.Args[0], ctx)
		if err != nil {
			return NullValue, err
		}
		if v.Type == TypeInt {
			return IntValue(int64(math.Abs(float64(v.Int)))), nil
		}
		if f, ok := v.AsFloat(); ok {
			return FloatValue(math.Abs(f)), nil
		}
		return NullValue, fmt.Errorf("abs requires number")
	case "lower", "upper":
		v, err := Eval(n.Args[0], ctx)
		if err != nil {
			return NullValue, err
		}
		if v.Null {
			return NullValue, nil
		}
		if name == "lower" {
			return StrValue(strings.ToLower(v.String())), nil
		}
		return StrValue(strings.ToUpper(v.String())), nil
	case "length", "len":
		v, err := Eval(n.Args[0], ctx)
		if err != nil {
			return NullValue, err
		}
		if v.Null {
			return NullValue, nil
		}
		return IntValue(int64(len([]rune(v.String())))), nil
	case "concat", "concat_ws":
		var sb strings.Builder
		for i, a := range n.Args {
			v, err := Eval(a, ctx)
			if err != nil {
				return NullValue, err
			}
			if v.Null {
				continue
			}
			if name == "concat_ws" && i > 0 {
				sb.WriteString(v.String())
			} else if name == "concat" && i > 0 {
				sb.WriteString(v.String())
			} else {
				sb.WriteString(v.String())
			}
		}
		return StrValue(sb.String()), nil
	case "substr", "substring":
		if len(n.Args) < 2 {
			return NullValue, fmt.Errorf("substr needs at least 2 args")
		}
		v, err := Eval(n.Args[0], ctx)
		if err != nil {
			return NullValue, err
		}
		start, err := Eval(n.Args[1], ctx)
		if err != nil {
			return NullValue, err
		}
		s := []rune(v.String())
		from := int(start.Int)
		if from < 1 {
			from = 1
		}
		if from > len(s) {
			return StrValue(""), nil
		}
		if len(n.Args) >= 3 {
			cnt, _ := Eval(n.Args[2], ctx)
			to := from + int(cnt.Int)
			if to > len(s)+1 {
				to = len(s) + 1
			}
			return StrValue(string(s[from-1 : to-1])), nil
		}
		return StrValue(string(s[from-1:])), nil
	case "now":
		return TimestampValue(time.Now()), nil
	case "unixepoch":
		return IntValue(time.Now().Unix()), nil
	}
	return NullValue, fmt.Errorf("unknown function %q", n.Name)
}

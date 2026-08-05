package sql

import (
	"fmt"
	"strings"
	"unicode"
)

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokString
	tokNumber
	tokOp
	tokLParen
	tokRParen
	tokComma
	tokStar
	tokSemicolon
	tokDot
)

type token struct {
	kind tokKind
	text string
	pos  int
	// numeric literal value
	isFloat bool
	intVal  int64
	fltVal  float64
}

type lexer struct {
	src string
	pos int
}

var keywords = map[string]bool{
	"select": true, "from": true, "where": true, "insert": true,
	"into": true, "values": true, "update": true, "set": true,
	"delete": true, "create": true, "table": true, "drop": true,
	"index": true, "and": true, "or": true, "not": true, "null": true,
	"true": true, "false": true, "as": true, "distinct": true,
	"group": true, "by": true, "having": true, "order": true,
	"limit": true, "begin": true, "commit": true, "rollback": true,
	"database": true, "if": true, "exists": true, "primary": true,
	"key": true, "default": true, "int": true, "float": true,
	"double": true, "string": true, "text": true, "bool": true,
	"timestamp": true, "desc": true, "asc": true, "cast": true,
	"in": true, "between": true, "like": true,
}

func (l *lexer) next() (token, error) {
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			l.pos++
			continue
		}
		break
	}
	start := l.pos
	if l.pos >= len(l.src) {
		return token{kind: tokEOF, pos: start}, nil
	}
	c := l.src[l.pos]
	switch {
	case c == ',':
		l.pos++
		return token{kind: tokComma, text: ",", pos: start}, nil
	case c == '(':
		l.pos++
		return token{kind: tokLParen, text: "(", pos: start}, nil
	case c == ')':
		l.pos++
		return token{kind: tokRParen, text: ")", pos: start}, nil
	case c == '*':
		l.pos++
		return token{kind: tokStar, text: "*", pos: start}, nil
	case c == ';':
		l.pos++
		return token{kind: tokSemicolon, text: ";", pos: start}, nil
	case c == '.':
		if l.pos+1 < len(l.src) && l.src[l.pos+1] >= '0' && l.src[l.pos+1] <= '9' {
			return l.lexNumber()
		}
		l.pos++
		return token{kind: tokDot, text: ".", pos: start}, nil
	case c == '\'' || c == '"':
		return l.lexString()
	case c >= '0' && c <= '9':
		return l.lexNumber()
	case c == '_' || unicode.IsLetter(rune(c)):
		return l.lexIdent()
	default:
		// operators: ==, >=, <=, <>, !=, and single chars
		two := ""
		if l.pos+1 < len(l.src) {
			two = l.src[l.pos : l.pos+2]
		}
		switch two {
		case "==", ">=", "<=", "<>", "!=":
			l.pos += 2
			return token{kind: tokOp, text: two, pos: start}, nil
		}
		if strings.ContainsRune("+-*/=<>", rune(c)) {
			l.pos++
			return token{kind: tokOp, text: string(c), pos: start}, nil
		}
		return token{}, fmt.Errorf("syntax error near %q at pos %d", string(c), start)
	}
}

func (l *lexer) lexIdent() (token, error) {
	start := l.pos
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '_' || unicode.IsLetter(rune(c)) || (l.pos > start && c >= '0' && c <= '9') {
			l.pos++
		} else {
			break
		}
	}
	text := l.src[start:l.pos]
	return token{kind: tokIdent, text: text, pos: start}, nil
}

func (l *lexer) lexString() (token, error) {
	start := l.pos
	quote := l.src[l.pos]
	l.pos++
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == quote {
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == quote {
				sb.WriteByte(quote)
				l.pos += 2
				continue
			}
			l.pos++
			return token{kind: tokString, text: sb.String(), pos: start}, nil
		}
		sb.WriteByte(c)
		l.pos++
	}
	return token{}, fmt.Errorf("unterminated string literal at pos %d", start)
}

func (l *lexer) lexNumber() (token, error) {
	start := l.pos
	isFloat := false
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c >= '0' && c <= '9' {
			l.pos++
		} else if c == '.' {
			isFloat = true
			l.pos++
		} else if c == 'e' || c == 'E' {
			isFloat = true
			l.pos++
			if l.pos < len(l.src) && (l.src[l.pos] == '+' || l.src[l.pos] == '-') {
				l.pos++
			}
		} else {
			break
		}
	}
	text := l.src[start:l.pos]
	t := token{kind: tokNumber, text: text, pos: start, isFloat: isFloat}
	if isFloat {
		var f float64
		if _, err := fmt.Sscanf(text, "%g", &f); err != nil {
			return t, fmt.Errorf("bad number %q", text)
		}
		t.fltVal = f
	} else {
		var i int64
		if _, err := fmt.Sscanf(text, "%d", &i); err != nil {
			return t, fmt.Errorf("bad integer %q", text)
		}
		t.intVal = i
	}
	return t, nil
}

// tokenize splits the whole input into tokens.
func tokenize(src string) ([]token, error) {
	l := &lexer{src: src}
	var out []token
	for {
		t, err := l.next()
		if err != nil {
			return nil, err
		}
		out = append(out, t)
		if t.kind == tokEOF {
			return out, nil
		}
	}
}

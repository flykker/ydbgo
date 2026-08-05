package sql

// SplitStatements splits a script into individual statements on top-level
// semicolons, honoring single/double quotes and parentheses.
func SplitStatements(src string) ([]string, error) {
	var stmts []string
	var cur []byte
	var quote byte
	depth := 0
	for i := 0; i < len(src); i++ {
		c := src[i]
		if quote != 0 {
			cur = append(cur, c)
			if c == quote {
				// handle doubled quotes
				if i+1 < len(src) && src[i+1] == quote {
					cur = append(cur, src[i+1])
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
			cur = append(cur, c)
		case '(':
			depth++
			cur = append(cur, c)
		case ')':
			if depth > 0 {
				depth--
			}
			cur = append(cur, c)
		case ';':
			if depth == 0 {
				stmts = append(stmts, string(cur))
				cur = nil
			} else {
				cur = append(cur, c)
			}
		default:
			cur = append(cur, c)
		}
	}
	if quote != 0 {
		return nil, &ExecError{Msg: "unterminated string literal"}
	}
	if depth != 0 {
		return nil, &ExecError{Msg: "unbalanced parentheses"}
	}
	if len(cur) > 0 {
		stmts = append(stmts, string(cur))
	}
	return stmts, nil
}

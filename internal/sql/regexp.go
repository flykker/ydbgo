package sql

import (
	"regexp"
	"strings"
)

var reCache = map[string]*regexp.Regexp{}

func newRe(pat string) func(string) bool {
	if re, ok := reCache[pat]; ok {
		return func(s string) bool { return re.MatchString(s) }
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return func(string) bool { return false }
	}
	reCache[pat] = re
	return func(s string) bool { return re.MatchString(s) }
}

// likeMatch reports whether s matches the SQL LIKE pattern pat, where '%'
// matches any run sequence and '_' matches a single rune. Literal metacharacters
// are quoted so the pattern is matched literally.
func likeMatch(s, pat string) bool {
	var sb strings.Builder
	sb.WriteString("^")
	for _, c := range pat {
		switch c {
		case '%':
			sb.WriteString(".*")
		case '_':
			sb.WriteString(".")
		default:
			sb.WriteString("\\Q")
			sb.WriteRune(c)
			sb.WriteString("\\E")
		}
	}
	sb.WriteString("$")
	return newRe(sb.String())(s)
}

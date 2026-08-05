package sql

import "regexp"

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

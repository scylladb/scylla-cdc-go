package scylla_cdc

import (
	"regexp"
	"strings"

	"github.com/gocql/gocql"
)

func compareTimeuuid(u1 gocql.UUID, u2 gocql.UUID) int {
	// Compare timestamps
	t1 := u1.Timestamp()
	t2 := u2.Timestamp()
	if t1 < t2 {
		return -1
	}
	if t1 > t2 {
		return 1
	}

	// Lexicographically compare the second half as signed bytes
	for i := 8; i < 16; i++ {
		d := int8(u1[i]) - int8(u2[i])
		if d != 0 {
			return int(d)
		}
	}
	return 0
}

var validIDPattern = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_]*$")

func escapeColumnNameIfNeeded(s string) string {
	if shouldEscape(s) {
		return escapeColumnName(s)
	}
	return s
}

func shouldEscape(s string) bool {
	// TODO: Check if it is a reserved keyword - for now, assume it's not
	return !validIDPattern.MatchString(s)
}

func escapeColumnName(s string) string {
	return "\"" + strings.ReplaceAll(s, "\"", "\\\"") + "\""
}

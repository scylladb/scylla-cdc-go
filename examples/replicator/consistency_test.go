package main

import (
	"testing"

	"github.com/gocql/gocql"
)

func TestParseConsistency(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  gocql.Consistency
	}{
		{name: "omitted", want: gocql.LocalQuorum},
		{name: "local quorum", input: "local_quorum", want: gocql.LocalQuorum},
		{name: "mixed case", input: "LoCaL_QuOrUm", want: gocql.LocalQuorum},
		{name: "one", input: "one", want: gocql.One},
		{name: "quorum", input: "quorum", want: gocql.Quorum},
		{name: "all", input: "all", want: gocql.All},
		{name: "uppercase", input: "ONE", want: gocql.One},
		{name: "unsupported", input: "unsupported", want: gocql.LocalQuorum},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseConsistency(tt.input); got != tt.want {
				t.Errorf("parseConsistency(%q) = %s, want %s", tt.input, got, tt.want)
			}
		})
	}
}

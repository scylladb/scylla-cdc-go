package scylla_cdc

import (
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

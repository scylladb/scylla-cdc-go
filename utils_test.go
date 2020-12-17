package scyllacdc

import (
	"testing"

	"github.com/gocql/gocql"
)

func TestTimeUUIDCompare(t *testing.T) {
	uuids := []string{
		"1f085b0c-3f3b-11eb-1f65-8af9c0d59390",
		"1f4e6278-3f3b-11eb-c486-5caaf85a5ff8",
		"200977b6-3f3b-11eb-b122-b0f477f765ec",
		"200977b6-3f3b-11eb-4230-d2afd2dadc1b",
		"2020daa0-3f3b-11eb-5459-3f3b971d3b10",
		"20fa9312-3f3b-11eb-da71-8cc350d31319",
	}

	for i := 0; i < len(uuids); i++ {
		u1, err := gocql.ParseUUID(uuids[i])
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < len(uuids); j++ {
			u2, err := gocql.ParseUUID(uuids[j])
			if err != nil {
				t.Fatal(err)
			}

			cmp := compareTimeuuid(u1, u2)

			if i < j && !(cmp < 0) {
				t.Errorf("expected %s to be smaller than %s", u1, u2)
			} else if i == j && !(cmp == 0) {
				t.Errorf("expected %s to be equal to %s", u1, u2)
			} else if i > j && !(cmp > 0) {
				t.Errorf("expected %s to be larger than %s", u1, u2)
			}
		}
	}
}

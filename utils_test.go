package scyllacdc

import (
	"testing"
	"time"

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

			cmp := CompareTimeUUID(u1, u2)

			switch {
			case i < j && cmp >= 0:
				t.Errorf("expected %s to be smaller than %s", u1, u2)
			case i == j && cmp != 0:
				t.Errorf("expected %s to be equal to %s", u1, u2)
			case i > j && cmp <= 0:
				t.Errorf("expected %s to be larger than %s", u1, u2)
			}
		}
	}
}

func TestBackoffDelay(t *testing.T) {
	base := 1 * time.Second
	maxDelay := 30 * time.Second

	tests := []struct {
		failures int
		expected time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second}, // capped
		{7, 30 * time.Second}, // stays capped
		{100, 30 * time.Second},
	}

	for _, tt := range tests {
		got := backoffDelay(base, maxDelay, tt.failures)
		if got != tt.expected {
			t.Errorf("backoffDelay(%v, %v, %d) = %v, want %v", base, maxDelay, tt.failures, got, tt.expected)
		}
	}
}

func TestBackoffDelayNoOverflow(t *testing.T) {
	base := time.Hour
	maxDelay := 24 * time.Hour

	got := backoffDelay(base, maxDelay, 1000)
	if got != maxDelay {
		t.Errorf("backoffDelay with extreme failures = %v, want %v (should cap, not overflow)", got, maxDelay)
	}
	if got < 0 {
		t.Errorf("backoffDelay overflowed to negative: %v", got)
	}
}

func TestAddJitter(t *testing.T) {
	d := 100 * time.Millisecond
	lo := d / 2
	hi := d/2 + d

	for range 1000 {
		jittered := addJitter(d)
		if jittered < lo || jittered > hi {
			t.Errorf("addJitter(%v) = %v, want in [%v, %v]", d, jittered, lo, hi)
		}
	}

	if got := addJitter(0); got != 0 {
		t.Errorf("addJitter(0) = %v, want 0", got)
	}
	if got := addJitter(-time.Second); got != -time.Second {
		t.Errorf("addJitter(-1s) = %v, want -1s", got)
	}
}

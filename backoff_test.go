package scyllacdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

type emptyIterator struct{}

func (emptyIterator) Next() (cdcChangeBatchCols, *ChangeRow) { return cdcChangeBatchCols{}, nil }
func (emptyIterator) Close() error                           { return nil }

type noopConsumer struct{}

func (noopConsumer) Consume(context.Context, Change) error { return nil }
func (noopConsumer) End() error                            { return nil }

type noopConsumerFactory struct{}

func (noopConsumerFactory) CreateChangeConsumer(context.Context, CreateChangeConsumerInput) (ChangeConsumer, error) {
	return noopConsumer{}, nil
}

// callTracker records timestamps of calls in a thread-safe manner.
type callTracker struct {
	mu    sync.Mutex
	times []time.Time
}

func (ct *callTracker) record() {
	ct.mu.Lock()
	ct.times = append(ct.times, time.Now())
	ct.mu.Unlock()
}

func (ct *callTracker) getTimes() []time.Time {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	cp := make([]time.Time, len(ct.times))
	copy(cp, ct.times)
	return cp
}

func (ct *callTracker) count() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return len(ct.times)
}

func newTestStreamBatchReader(
	baseDelay, maxDelay time.Duration,
	queryFn func(begin, end gocql.UUID) (cdcIterator, error),
) *streamBatchReader {
	config := &ReaderConfig{
		ChangeConsumerFactory: noopConsumerFactory{},
		ProgressManager:       noProgressManager{},
		Logger:                noLogger{},
		Advanced: AdvancedReaderConfig{
			PostFailedQueryDelay:    baseDelay,
			MaxPostFailedQueryDelay: maxDelay,
			PostEmptyQueryDelay:     baseDelay,
			PostNonEmptyQueryDelay:  baseDelay,
			ConfidenceWindowSize:    time.Millisecond,
			QueryTimeWindowSize:     365 * 24 * time.Hour,
			TableMissingRetryLimit:  30,
		},
	}

	startFrom := time.Now().Add(-time.Hour)
	streams := []StreamID{[]byte("0123456789abcdef")} // 16 bytes

	sbr := newStreamBatchReader(
		config,
		startFrom,
		streams,
		"test_ks",
		"test_tbl",
		gocql.MinTimeUUID(startFrom),
	)
	sbr.queryRangeFunc = queryFn
	return sbr
}

func TestStreamBatchReaderBackoffOnConsecutiveFailures(t *testing.T) {
	tracker := &callTracker{}
	baseDelay := 5 * time.Millisecond
	maxDelay := 80 * time.Millisecond

	sbr := newTestStreamBatchReader(baseDelay, maxDelay, func(begin, end gocql.UUID) (cdcIterator, error) {
		tracker.record()
		return nil, errors.New("connection refused")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := sbr.run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	times := tracker.getTimes()
	if len(times) < 4 {
		t.Fatalf("expected at least 4 query attempts, got %d", len(times))
	}

	if len(times) > 50 {
		t.Errorf("too many query attempts (%d), backoff not working", len(times))
	}

	intervals := computeIntervals(times)
	earlyAvg := avgDuration(intervals[:3])
	lateAvg := avgDuration(intervals[len(intervals)-3:])

	if lateAvg <= earlyAvg {
		t.Errorf("late average interval (%v) should be larger than early average (%v)", lateAvg, earlyAvg)
	}

	if lateAvg < maxDelay/4 {
		t.Errorf("late average interval (%v) should approach max delay (%v)", lateAvg, maxDelay)
	}
}

func TestStreamBatchReaderBackoffResetOnSuccess(t *testing.T) {
	tracker := &callTracker{}
	baseDelay := 5 * time.Millisecond
	maxDelay := 160 * time.Millisecond

	const failsBeforeSuccess = 5

	var mu sync.Mutex
	callCount := 0
	queryFn := func(begin, end gocql.UUID) (cdcIterator, error) {
		tracker.record()
		mu.Lock()
		n := callCount
		callCount++
		mu.Unlock()

		if n == failsBeforeSuccess {
			return emptyIterator{}, nil
		}
		return nil, errors.New("connection refused")
	}

	sbr := newTestStreamBatchReader(baseDelay, maxDelay, queryFn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := sbr.run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	times := tracker.getTimes()
	minCalls := failsBeforeSuccess + 3
	if len(times) < minCalls {
		t.Fatalf("expected at least %d query attempts, got %d", minCalls, len(times))
	}

	intervals := computeIntervals(times)
	successIdx := failsBeforeSuccess
	if len(intervals) <= successIdx+1 {
		t.Fatalf("need at least %d intervals, got %d", successIdx+2, len(intervals))
	}

	intervalBeforeSuccess := intervals[successIdx-1]
	intervalAfterSuccess := intervals[successIdx]
	intervalFirstErrorAfterReset := intervals[successIdx+1]

	if intervalBeforeSuccess < baseDelay*2 {
		t.Errorf("interval before success (%v) should show backoff (>= %v)", intervalBeforeSuccess, baseDelay*2)
	}

	if intervalAfterSuccess >= intervalBeforeSuccess {
		t.Errorf("interval after success (%v) should be shorter than before (%v) — backoff not resetting",
			intervalAfterSuccess, intervalBeforeSuccess)
	}

	tolerance := baseDelay * 5
	if intervalAfterSuccess > tolerance {
		t.Errorf("interval after success (%v) should be close to base delay (%v)", intervalAfterSuccess, baseDelay)
	}
	if intervalFirstErrorAfterReset > tolerance {
		t.Errorf("first failure after reset (%v) should be close to base delay (%v)", intervalFirstErrorAfterReset, baseDelay)
	}
}

func TestStreamBatchReaderTableMissingGivesUp(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "ErrNotFound (original table deleted)",
			err:  fmt.Errorf("failed to get CDC table columns: %w", gocql.ErrNotFound),
		},
		{
			name: "unconfigured table (CDC table deleted)",
			err:  fmt.Errorf("unconfigured table test_ks.test_tbl_scylla_cdc_log"),
		},
		{
			name: "no such table",
			err:  fmt.Errorf("no such table test_ks.test_tbl_scylla_cdc_log"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &callTracker{}

			sbr := newTestStreamBatchReader(time.Millisecond, 5*time.Millisecond,
				func(begin, end gocql.UUID) (cdcIterator, error) {
					tracker.record()
					return nil, tt.err
				},
			)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := sbr.run(ctx)

			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("reader should have given up before context timeout")
			}
			if err == nil {
				t.Fatal("expected an error from table-missing retry limit")
			}
			if !errors.Is(err, tt.err) {
				t.Logf("returned error: %v", err)
			}

			count := tracker.count()
			if count != 31 {
				t.Errorf("expected exactly 31 attempts (30 retries + 1 final), got %d", count)
			}
		})
	}
}

func TestStreamBatchReaderGenericErrorDoesNotGiveUp(t *testing.T) {
	tracker := &callTracker{}

	sbr := newTestStreamBatchReader(time.Millisecond, 5*time.Millisecond,
		func(begin, end gocql.UUID) (cdcIterator, error) {
			tracker.record()
			return nil, errors.New("connection refused")
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := sbr.run(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded (reader should not give up on generic errors), got: %v", err)
	}

	count := tracker.count()
	if count <= 31 {
		t.Errorf("expected more than 31 attempts to prove no retry limit was hit, got %d", count)
	}
}

func TestStreamBatchReaderTableMissingResetsOnSuccess(t *testing.T) {
	tracker := &callTracker{}
	var mu sync.Mutex
	callCount := 0

	// Return table-missing for 25 calls, then success, then table-missing forever.
	// After reset, it should survive another 30 retries.
	queryFn := func(begin, end gocql.UUID) (cdcIterator, error) {
		tracker.record()
		mu.Lock()
		n := callCount
		callCount++
		mu.Unlock()

		if n == 25 {
			return emptyIterator{}, nil
		}
		return nil, gocql.ErrNotFound
	}

	sbr := newTestStreamBatchReader(time.Millisecond, 5*time.Millisecond, queryFn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := sbr.run(ctx)

	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("reader should have given up before context timeout")
	}
	if err == nil {
		t.Fatal("expected table-missing error")
	}

	// Total calls: 25 (table missing) + 1 (success) + 31 (table missing, gives up on 31st) = 57
	count := tracker.count()
	if count != 57 {
		t.Errorf("expected 57 attempts (25 + 1 success + 31), got %d", count)
	}
}

type mockGenerationSource struct {
	mu         sync.Mutex
	getTimesFn func(gocql.Consistency) ([]time.Time, error)
}

func (m *mockGenerationSource) getGenerationTimes(consistency gocql.Consistency) ([]time.Time, error) {
	m.mu.Lock()
	fn := m.getTimesFn
	m.mu.Unlock()
	return fn(consistency)
}

func (m *mockGenerationSource) getGeneration(genTime time.Time, consistency gocql.Consistency) ([][]StreamID, error) {
	return nil, nil
}

func (m *mockGenerationSource) maybeUpgrade() (generationSource, error) {
	return m, nil
}

func newTestGenerationFetcher(
	basePeriod, maxPeriod time.Duration,
	source generationSource,
) *generationFetcher {
	return &generationFetcher{
		logger:   noLogger{},
		lastTime: time.Now().Add(-time.Hour),
		source:   source,

		generationCh: make(chan *generation, 1),
		stopCh:       make(chan struct{}),
		refreshCh:    make(chan struct{}, 1),

		fetchPeriod:    basePeriod,
		maxFetchPeriod: maxPeriod,

		clusterSizeFunc: func() (int, error) { return 3, nil },
	}
}

func TestGenerationFetcherBackoffOnConsecutiveFailures(t *testing.T) {
	tracker := &callTracker{}
	basePeriod := 5 * time.Millisecond
	maxPeriod := 80 * time.Millisecond

	source := &mockGenerationSource{
		getTimesFn: func(gocql.Consistency) ([]time.Time, error) {
			tracker.record()
			return nil, errors.New("connection refused")
		},
	}

	gf := newTestGenerationFetcher(basePeriod, maxPeriod, source)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = gf.Run(ctx)

	times := tracker.getTimes()
	if len(times) < 4 {
		t.Fatalf("expected at least 4 fetch attempts, got %d", len(times))
	}

	if len(times) > 50 {
		t.Errorf("too many fetch attempts (%d), backoff not working", len(times))
	}

	intervals := computeIntervals(times)
	earlyAvg := avgDuration(intervals[:3])
	lateAvg := avgDuration(intervals[len(intervals)-3:])

	if lateAvg <= earlyAvg {
		t.Errorf("late average interval (%v) should be larger than early average (%v)", lateAvg, earlyAvg)
	}

	if lateAvg < maxPeriod/4 {
		t.Errorf("late average interval (%v) should approach max period (%v)", lateAvg, maxPeriod)
	}
}

func TestGenerationFetcherBackoffResetOnSuccess(t *testing.T) {
	tracker := &callTracker{}
	basePeriod := 5 * time.Millisecond
	maxPeriod := 160 * time.Millisecond

	const failsBeforeSuccess = 5

	var mu sync.Mutex
	callCount := 0

	source := &mockGenerationSource{
		getTimesFn: func(gocql.Consistency) ([]time.Time, error) {
			tracker.record()
			mu.Lock()
			n := callCount
			callCount++
			mu.Unlock()

			if n == failsBeforeSuccess {
				return nil, nil
			}
			return nil, errors.New("connection refused")
		},
	}

	gf := newTestGenerationFetcher(basePeriod, maxPeriod, source)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = gf.Run(ctx)

	times := tracker.getTimes()
	minCalls := failsBeforeSuccess + 3
	if len(times) < minCalls {
		t.Fatalf("expected at least %d fetch attempts, got %d", minCalls, len(times))
	}

	intervals := computeIntervals(times)
	successIdx := failsBeforeSuccess
	if len(intervals) <= successIdx+1 {
		t.Fatalf("need at least %d intervals, got %d", successIdx+2, len(intervals))
	}

	intervalBeforeSuccess := intervals[successIdx-1]
	intervalAfterSuccess := intervals[successIdx]
	intervalFirstErrorAfterReset := intervals[successIdx+1]

	if intervalBeforeSuccess < basePeriod*2 {
		t.Errorf("interval before success (%v) should show backoff (>= %v)", intervalBeforeSuccess, basePeriod*2)
	}

	if intervalAfterSuccess >= intervalBeforeSuccess {
		t.Errorf("interval after success (%v) should be shorter than before (%v) — backoff not resetting",
			intervalAfterSuccess, intervalBeforeSuccess)
	}

	tolerance := basePeriod * 5
	if intervalAfterSuccess > tolerance {
		t.Errorf("interval after success (%v) should be close to base period (%v)", intervalAfterSuccess, basePeriod)
	}
	if intervalFirstErrorAfterReset > tolerance {
		t.Errorf("first failure after reset (%v) should be close to base period (%v)", intervalFirstErrorAfterReset, basePeriod)
	}
}

func TestGenerationFetcherClusterSizeErrorBackoff(t *testing.T) {
	tracker := &callTracker{}
	basePeriod := 5 * time.Millisecond
	maxPeriod := 80 * time.Millisecond

	source := &mockGenerationSource{
		getTimesFn: func(gocql.Consistency) ([]time.Time, error) {
			// Should not be called because getClusterSize fails first
			t.Error("getGenerationTimes should not be called when getClusterSize fails")
			return nil, nil
		},
	}

	gf := newTestGenerationFetcher(basePeriod, maxPeriod, source)
	gf.clusterSizeFunc = func() (int, error) {
		tracker.record()
		return 0, errors.New("cannot reach cluster")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = gf.Run(ctx)

	times := tracker.getTimes()
	if len(times) < 4 {
		t.Fatalf("expected at least 4 attempts, got %d", len(times))
	}

	if len(times) > 50 {
		t.Errorf("too many attempts (%d), backoff not working for cluster size errors", len(times))
	}

	intervals := computeIntervals(times)
	earlyAvg := avgDuration(intervals[:2])
	lateAvg := avgDuration(intervals[len(intervals)-2:])
	if lateAvg <= earlyAvg {
		t.Errorf("late average interval (%v) should be larger than early average (%v)", lateAvg, earlyAvg)
	}
}

func computeIntervals(times []time.Time) []time.Duration {
	intervals := make([]time.Duration, 0, len(times)-1)
	for i := 1; i < len(times); i++ {
		intervals = append(intervals, times[i].Sub(times[i-1]))
	}
	return intervals
}

func avgDuration(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	return sum / time.Duration(len(ds))
}

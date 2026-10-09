package scyllacdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

type singleChangeIterator struct {
	stream   StreamID
	when     gocql.UUID
	read     bool
	closeErr error
}

func (it *singleChangeIterator) Next() (cdcChangeBatchCols, *ChangeRow) {
	if it.read {
		return cdcChangeBatchCols{}, nil
	}
	it.read = true
	return cdcChangeBatchCols{streamID: it.stream, time: it.when},
		&ChangeRow{cdcCols: cdcChangeRowCols{operation: int8(Insert), endOfBatch: true}}
}

func (it *singleChangeIterator) Close() error { return it.closeErr }

type savedStreamProgress struct {
	noProgressManager
	values map[string]Progress
}

func (pm *savedStreamProgress) GetProgress(_ context.Context, _ time.Time, _ string, stream StreamID) (Progress, error) {
	return pm.values[string(stream)], nil
}

func (pm *savedStreamProgress) SaveProgress(_ context.Context, _ time.Time, _ string, stream StreamID, progress Progress) error {
	pm.values[string(stream)] = progress
	return nil
}

type checkpointConsumerFactory struct {
	consumers    map[string]*checkpointConsumer
	failEmptyFor string
	onEmptyFor   string
	onEmpty      func()
}

func (f *checkpointConsumerFactory) CreateChangeConsumer(_ context.Context, input CreateChangeConsumerInput) (ChangeConsumer, error) {
	c := &checkpointConsumer{reporter: input.ProgressReporter, failEmptyOnce: string(input.StreamID) == f.failEmptyFor}
	if string(input.StreamID) == f.onEmptyFor {
		c.onEmpty = f.onEmpty
	}
	f.consumers[string(input.StreamID)] = c
	return c, nil
}

type checkpointConsumer struct {
	reporter      *ProgressReporter
	changes       int
	empties       int
	ackTimes      []gocql.UUID
	failEmptyOnce bool
	onEmpty       func()
}

func (c *checkpointConsumer) Consume(ctx context.Context, change Change) error {
	c.changes++
	return c.reporter.MarkProgress(ctx, Progress{LastProcessedRecordTime: change.Time})
}

func (c *checkpointConsumer) Empty(ctx context.Context, ackTime gocql.UUID) error {
	c.empties++
	c.ackTimes = append(c.ackTimes, ackTime)
	if c.failEmptyOnce {
		c.failEmptyOnce = false
		return errors.New("checkpoint failed")
	}
	if err := c.reporter.MarkProgress(ctx, Progress{LastProcessedRecordTime: ackTime}); err != nil {
		return err
	}
	if c.onEmpty != nil {
		c.onEmpty()
	}
	return nil
}

func (c *checkpointConsumer) End() error { return nil }

func TestMixedBatchCheckpointsIdleStreamAcrossRestart(t *testing.T) {
	busy := StreamID("busy")
	idle := StreamID("idle")
	start := time.Now().Add(-time.Hour)
	end := start.Add(time.Second)
	changeTime := gocql.MinTimeUUID(start.Add(500 * time.Millisecond))
	pm := &savedStreamProgress{values: make(map[string]Progress)}
	factory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	config := &ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                noLogger{},
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:    time.Second,
			ConfidenceWindowSize:   time.Second,
			PostNonEmptyQueryDelay: time.Millisecond,
		},
	}
	newBatch := func() *streamBatchReader {
		return newStreamBatchReader(config, start.Add(-24*time.Hour), []StreamID{busy, idle},
			"ks", "tbl", gocql.MinTimeUUID(start))
	}
	batch := newBatch()
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		return &singleChangeIterator{stream: busy, when: changeTime}, nil
	}
	batch.close(gocql.MinTimeUUID(end))
	if err := batch.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := factory.consumers[string(busy)]; got.changes != 1 || got.empties != 0 {
		t.Fatalf("busy stream got %d changes and %d empty callbacks", got.changes, got.empties)
	}
	if got := factory.consumers[string(idle)]; got.changes != 0 || got.empties != 1 {
		t.Fatalf("idle stream got %d changes and %d empty callbacks", got.changes, got.empties)
	}
	if got := pm.values[string(idle)].LastProcessedRecordTime; CompareTimeUUID(got, gocql.MinTimeUUID(end)) != 0 {
		t.Fatalf("idle checkpoint = %s, want %s", got, gocql.MinTimeUUID(end))
	}
	savedCheckpoints := map[string]gocql.UUID{
		string(busy): pm.values[string(busy)].LastProcessedRecordTime,
		string(idle): pm.values[string(idle)].LastProcessedRecordTime,
	}

	// A restarted batch filters the saved change and only checkpoints later windows.
	restartedFactory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	restartedConfig := *config
	restartedConfig.ChangeConsumerFactory = restartedFactory
	restarted := newStreamBatchReader(&restartedConfig, start.Add(-24*time.Hour), []StreamID{busy, idle},
		"ks", "tbl", gocql.MinTimeUUID(start))
	restarted.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		return &singleChangeIterator{stream: busy, when: changeTime}, nil
	}
	restarted.close(gocql.MinTimeUUID(end.Add(time.Second)))
	if err := restarted.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []StreamID{busy, idle} {
		consumer := restartedFactory.consumers[string(stream)]
		if consumer.changes != 0 {
			t.Errorf("restarted stream %s consumed %d saved changes", stream, consumer.changes)
		}
		checkpoint := savedCheckpoints[string(stream)]
		if len(consumer.ackTimes) == 0 {
			t.Errorf("restarted stream %s received no later empty checkpoint", stream)
		}
		for _, ack := range consumer.ackTimes {
			if CompareTimeUUID(ack, checkpoint) <= 0 {
				t.Errorf("restarted stream %s acknowledged %s at or before saved progress %s", stream, ack, checkpoint)
			}
		}
	}
}

func TestMixedBatchDoesNotCheckpointAfterFailedQuery(t *testing.T) {
	busy := StreamID("busy")
	idle := StreamID("idle")
	start := time.Now().Add(-time.Minute)
	pm := &savedStreamProgress{values: make(map[string]Progress)}
	factory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	config := &ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                noLogger{},
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:     time.Second,
			ConfidenceWindowSize:    time.Second,
			PostFailedQueryDelay:    time.Millisecond,
			MaxPostFailedQueryDelay: time.Millisecond,
		},
	}
	batch := newStreamBatchReader(config, start, []StreamID{busy, idle}, "ks", "tbl", gocql.MinTimeUUID(start))
	queryCount := 0
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		queryCount++
		if queryCount == 1 {
			return &singleChangeIterator{stream: busy, when: gocql.MinTimeUUID(start.Add(500 * time.Millisecond)), closeErr: errors.New("page failed")}, nil
		}
		return &singleChangeIterator{stream: busy, when: gocql.MinTimeUUID(start.Add(500 * time.Millisecond))}, nil
	}
	batch.close(gocql.MinTimeUUID(start.Add(time.Second)))
	if err := batch.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queryCount != 2 {
		t.Fatalf("queries = %d, want retry after failed close", queryCount)
	}
	if got := factory.consumers[string(idle)].empties; got != 1 {
		t.Fatalf("idle stream got %d empty callbacks, want one after successful query", got)
	}
	if got := factory.consumers[string(busy)]; got.changes != 1 || got.empties != 1 {
		t.Fatalf("busy stream got %d changes and %d empty callbacks, want one each", got.changes, got.empties)
	}
}

func TestEmptyCheckpointFailureAdvancesWindow(t *testing.T) {
	acknowledged := StreamID("acknowledged")
	idle := StreamID("idle")
	start := time.Now().Add(-time.Minute)
	pm := &savedStreamProgress{values: make(map[string]Progress)}
	var logs bytes.Buffer
	factory := &checkpointConsumerFactory{
		consumers: make(map[string]*checkpointConsumer), failEmptyFor: string(idle),
	}
	config := &ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                log.New(&logs, "", 0),
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:     time.Second,
			ConfidenceWindowSize:    time.Second,
			PostEmptyQueryDelay:     time.Millisecond,
			PostFailedQueryDelay:    time.Second,
			MaxPostFailedQueryDelay: time.Second,
		},
	}
	batch := newStreamBatchReader(config, start, []StreamID{acknowledged, idle}, "ks", "tbl", gocql.MinTimeUUID(start))
	var begins []gocql.UUID
	batch.queryRangeFunc = func(begin, _ gocql.UUID) (cdcIterator, error) {
		begins = append(begins, begin)
		return emptyIterator{}, nil
	}
	batch.close(gocql.MinTimeUUID(start.Add(2 * time.Second)))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := batch.run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(begins) != 2 || CompareTimeUUID(begins[1], gocql.MinTimeUUID(start.Add(time.Second))) != 0 {
		t.Fatalf("query begins = %v, want the next window after failed checkpoint", begins)
	}
	if got := factory.consumers[string(idle)].empties; got != 2 {
		t.Fatalf("idle stream got %d empty callbacks, want two", got)
	}
	if got := factory.consumers[string(idle)].ackTimes; CompareTimeUUID(got[1], got[0]) <= 0 {
		t.Fatalf("idle stream ack times = %v, want later retry", got)
	}
	if got := factory.consumers[string(acknowledged)].empties; got != 2 {
		t.Fatalf("acknowledged stream got %d empty callbacks, want two", got)
	}
	if got := batch.perStreamProgress[string(idle)]; CompareTimeUUID(got, gocql.MinTimeUUID(start.Add(2*time.Second))) != 0 {
		t.Fatalf("idle stream progress = %s, want window end", got)
	}
	if got := pm.values[string(idle)].LastProcessedRecordTime; CompareTimeUUID(got, gocql.MinTimeUUID(start.Add(2*time.Second))) != 0 {
		t.Fatalf("recovered checkpoint = %s, want second window end", got)
	}
	if !bytes.Contains(logs.Bytes(), []byte("error while acknowledging empty window ending at ")) ||
		!bytes.Contains(logs.Bytes(), []byte("for 1 streams (first: 69646c65): checkpoint failed")) {
		t.Fatalf("missing checkpoint error log: %s", logs.String())
	}
	if got := bytes.Count(logs.Bytes(), []byte("error while acknowledging empty window")); got != 1 {
		t.Fatalf("checkpoint failure logs = %d, want one: %s", got, logs.String())
	}
}

func TestCancelledWindowSkipsEmptyCheckpoint(t *testing.T) {
	stream := StreamID("idle")
	start := time.Now().Add(-time.Minute)
	pm := &savedStreamProgress{values: make(map[string]Progress)}
	factory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	var logs bytes.Buffer
	batch := newStreamBatchReader(&ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                log.New(&logs, "", 0),
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:  time.Second,
			ConfidenceWindowSize: time.Second,
		},
	}, start, []StreamID{stream}, "ks", "tbl", gocql.MinTimeUUID(start))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		cancel()
		return emptyIterator{}, nil
	}
	if err := batch.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if got := factory.consumers[string(stream)].empties; got != 0 {
		t.Fatalf("empty callbacks = %d after cancellation", got)
	}
	if bytes.Contains(logs.Bytes(), []byte("acknowledging empty window")) {
		t.Fatalf("cancellation logged checkpoint failure: %s", logs.String())
	}
}

func TestCancellationBetweenEmptyCheckpointsStopsWindow(t *testing.T) {
	first := StreamID("first")
	second := StreamID("second")
	third := StreamID("third")
	start := time.Now().Add(-time.Minute)
	pm := &savedStreamProgress{values: make(map[string]Progress)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs bytes.Buffer
	factory := &checkpointConsumerFactory{
		consumers: make(map[string]*checkpointConsumer), failEmptyFor: string(first),
		onEmptyFor: string(second), onEmpty: cancel,
	}
	batch := newStreamBatchReader(&ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                log.New(&logs, "", 0),
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:  time.Second,
			ConfidenceWindowSize: time.Second,
		},
	}, start, []StreamID{first, second, third}, "ks", "tbl", gocql.MinTimeUUID(start))
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		return emptyIterator{}, nil
	}
	if err := batch.run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if got := factory.consumers[string(first)].empties; got != 1 {
		t.Fatalf("first stream empty callbacks = %d, want one", got)
	}
	if got := factory.consumers[string(second)].empties; got != 1 {
		t.Fatalf("second stream empty callbacks = %d, want one", got)
	}
	if got := factory.consumers[string(third)].empties; got != 0 {
		t.Fatalf("third stream empty callbacks = %d after cancellation", got)
	}
	if got := batch.perStreamProgress[string(third)]; CompareTimeUUID(got, gocql.MinTimeUUID(start)) != 0 {
		t.Fatalf("third stream advanced to %s after cancellation", got)
	}
	if got := bytes.Count(logs.Bytes(), []byte("error while acknowledging empty window")); got != 1 {
		t.Fatalf("checkpoint failure logs = %d, want one: %s", got, logs.String())
	}
	if !bytes.Contains(logs.Bytes(), []byte("checkpoint failed")) {
		t.Fatalf("missing earlier checkpoint failure: %s", logs.String())
	}
}

func TestMixedBatchDoesNotMoveCheckpointBackwards(t *testing.T) {
	busy := StreamID("busy")
	ahead := StreamID("ahead")
	start := time.Now().Add(-time.Minute)
	end := gocql.MinTimeUUID(start.Add(time.Second))
	aheadTime := gocql.MinTimeUUID(start.Add(2 * time.Second))
	pm := &savedStreamProgress{values: map[string]Progress{
		string(ahead): {LastProcessedRecordTime: aheadTime},
	}}
	factory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	batch := newStreamBatchReader(&ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                noLogger{},
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:    time.Second,
			ConfidenceWindowSize:   time.Second,
			PostNonEmptyQueryDelay: time.Millisecond,
		},
	}, start, []StreamID{busy, ahead}, "ks", "tbl", gocql.MinTimeUUID(start))
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		return &singleChangeIterator{stream: busy, when: gocql.MinTimeUUID(start.Add(500 * time.Millisecond))}, nil
	}
	batch.close(end)
	if err := batch.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := factory.consumers[string(ahead)].empties; got != 0 {
		t.Fatalf("ahead stream got %d empty callbacks, want none", got)
	}
	if got := pm.values[string(ahead)].LastProcessedRecordTime; CompareTimeUUID(got, aheadTime) != 0 {
		t.Fatalf("ahead checkpoint moved from %s to %s", aheadTime, got)
	}
}

func TestMixedBatchCheckpointsStreamWithOnlyFilteredRows(t *testing.T) {
	busy := StreamID("busy")
	filtered := StreamID("filtered")
	start := time.Now().Add(-time.Minute)
	end := gocql.MinTimeUUID(start.Add(time.Second))
	pm := &savedStreamProgress{values: map[string]Progress{
		string(filtered): {LastProcessedRecordTime: gocql.MinTimeUUID(start.Add(500 * time.Millisecond))},
	}}
	factory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	batch := newStreamBatchReader(&ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                noLogger{},
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:    time.Second,
			ConfidenceWindowSize:   time.Second,
			PostNonEmptyQueryDelay: time.Millisecond,
		},
	}, start, []StreamID{busy, filtered}, "ks", "tbl", gocql.MinTimeUUID(start))
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		return &singleChangeIterator{stream: filtered, when: gocql.MinTimeUUID(start.Add(250 * time.Millisecond))}, nil
	}
	batch.close(end)
	if err := batch.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := factory.consumers[string(filtered)]; got.changes != 0 || got.empties != 1 {
		t.Fatalf("filtered stream got %d changes and %d empty callbacks", got.changes, got.empties)
	}
	if got := pm.values[string(filtered)].LastProcessedRecordTime; CompareTimeUUID(got, end) != 0 {
		t.Fatalf("filtered stream checkpoint = %s, want %s", got, end)
	}
}

func TestFilteredRowsUseNonEmptyPollDelay(t *testing.T) {
	filtered := StreamID("filtered")
	idle := StreamID("idle")
	start := time.Now().Add(-time.Minute)
	pm := &savedStreamProgress{values: map[string]Progress{
		string(filtered): {LastProcessedRecordTime: gocql.MinTimeUUID(start.Add(500 * time.Millisecond))},
	}}
	factory := &checkpointConsumerFactory{consumers: make(map[string]*checkpointConsumer)}
	batch := newStreamBatchReader(&ReaderConfig{
		ChangeConsumerFactory: factory,
		ProgressManager:       pm,
		Logger:                noLogger{},
		Advanced: AdvancedReaderConfig{
			QueryTimeWindowSize:    time.Second,
			ConfidenceWindowSize:   time.Second,
			PostEmptyQueryDelay:    time.Hour,
			PostNonEmptyQueryDelay: time.Millisecond,
		},
	}, start, []StreamID{filtered, idle}, "ks", "tbl", gocql.MinTimeUUID(start))
	queries := 0
	batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
		queries++
		if queries == 1 {
			return &singleChangeIterator{stream: filtered, when: gocql.MinTimeUUID(start.Add(250 * time.Millisecond))}, nil
		}
		return emptyIterator{}, nil
	}
	batch.close(gocql.MinTimeUUID(start.Add(2 * time.Second)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := batch.run(ctx); err != nil {
		t.Fatal(err)
	}
	if queries != 2 {
		t.Fatalf("queries = %d, want two windows", queries)
	}
}

// mockRequestError implements gocql.RequestError for testing.
type mockRequestError struct {
	code    int
	message string
}

func (e *mockRequestError) Error() string   { return e.message }
func (e *mockRequestError) Code() int       { return e.code }
func (e *mockRequestError) Message() string { return e.message }

// Verify mockRequestError implements gocql.RequestError.
var _ gocql.RequestError = (*mockRequestError)(nil)

func TestIsTableMissingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrNotFound",
			err:      gocql.ErrNotFound,
			expected: true,
		},
		{
			name:     "wrapped ErrNotFound",
			err:      fmt.Errorf("some context: %w", gocql.ErrNotFound),
			expected: true,
		},
		{
			name:     "ErrKeyspaceDoesNotExist",
			err:      gocql.ErrKeyspaceDoesNotExist,
			expected: true,
		},
		{
			name:     "wrapped ErrKeyspaceDoesNotExist",
			err:      fmt.Errorf("some context: %w", gocql.ErrKeyspaceDoesNotExist),
			expected: true,
		},
		{
			name:     "RequestError with no such table",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "no such table ks.tbl"},
			expected: true,
		},
		{
			name:     "RequestError with unconfigured table",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "unconfigured table tbl"},
			expected: true,
		},
		{
			name:     "RequestError with does not exist",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "table ks.tbl does not exist"},
			expected: true,
		},
		{
			name:     "RequestError with wrong code but matching message falls through to string match",
			err:      &mockRequestError{code: gocql.ErrCodeSyntax, message: "no such table ks.tbl"},
			expected: true,
		},
		{
			name:     "RequestError with unrelated message",
			err:      &mockRequestError{code: gocql.ErrCodeInvalid, message: "invalid query syntax"},
			expected: false,
		},
		{
			name:     "generic error with no such table",
			err:      errors.New("no such table ks.tbl"),
			expected: true,
		},
		{
			name:     "generic error with unconfigured table",
			err:      errors.New("unconfigured table tbl"),
			expected: true,
		},
		{
			name:     "generic error with does not exist",
			err:      errors.New("keyspace does not exist"),
			expected: true,
		},
		{
			name:     "unrelated error",
			err:      errors.New("connection refused"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTableMissingError(tt.err)
			if got != tt.expected {
				t.Errorf("isTableMissingError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

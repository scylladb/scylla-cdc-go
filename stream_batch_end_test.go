package scyllacdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

type endErrorConsumer struct{ err error }

func (*endErrorConsumer) Consume(context.Context, Change) error { return nil }
func (c *endErrorConsumer) End() error                          { return c.err }

type endErrorFactory struct {
	consumer  *endErrorConsumer
	consumers map[string]*endErrorConsumer
}

func (f *endErrorFactory) CreateChangeConsumer(_ context.Context, input CreateChangeConsumerInput) (ChangeConsumer, error) {
	if f.consumers != nil {
		return f.consumers[string(input.StreamID)], nil
	}
	return f.consumer, nil
}

type endErrorByGenerationFactory struct {
	generation time.Time
	err        error
}

func (f *endErrorByGenerationFactory) CreateChangeConsumer(_ context.Context, input CreateChangeConsumerInput) (ChangeConsumer, error) {
	if input.ProgressReporter.gen.Equal(f.generation) {
		return &endErrorConsumer{err: f.err}, nil
	}
	return &endErrorConsumer{}, nil
}

type fixedGenerationSource struct{ times []time.Time }

func (s *fixedGenerationSource) getGenerationTimes(gocql.Consistency) ([]time.Time, error) {
	return s.times, nil
}

func (*fixedGenerationSource) getGeneration(time.Time, gocql.Consistency) ([][]StreamID, error) {
	return [][]StreamID{{StreamID("one")}}, nil
}

func (s *fixedGenerationSource) maybeUpgrade() (generationSource, error) { return s, nil }

type stopAfterGenerationProgressManager struct {
	noProgressManager
	stopAt time.Time
	stop   func()
	seen   []time.Time
}

func (pm *stopAfterGenerationProgressManager) StartGeneration(_ context.Context, gen time.Time) error {
	pm.seen = append(pm.seen, gen)
	if gen.Equal(pm.stopAt) {
		pm.stop()
	}
	return nil
}

func TestReaderRunContinuesAfterCheckpointFailureAtGenerationSwitch(t *testing.T) {
	first := time.Now().Add(-10 * time.Second)
	second := first.Add(5 * time.Second)
	source := &fixedGenerationSource{times: []time.Time{first, second}}
	progress := &stopAfterGenerationProgressManager{stopAt: second}
	reader := &Reader{
		config: &ReaderConfig{
			TableNames: []string{"ks.tbl"},
			ChangeConsumerFactory: &endErrorByGenerationFactory{
				generation: first,
				err:        &EndCheckpointError{Err: errors.New("checkpoint write timed out")},
			},
			ProgressManager: progress,
			Logger:          noLogger{},
			Advanced: AdvancedReaderConfig{
				QueryTimeWindowSize:  time.Minute,
				ConfidenceWindowSize: time.Second,
			},
		},
		genFetcher: newTestGenerationFetcher(time.Hour, time.Hour, source),
		readFrom:   first,
		stoppedCh:  make(chan struct{}),
		fetchTTLFunc: func(*gocql.Session, string, string) (int64, error) {
			return 0, nil
		},
		queryRangeFunc: func(gocql.UUID, gocql.UUID) (cdcIterator, error) {
			return emptyIterator{}, nil
		},
	}
	progress.stop = reader.Stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reader.Run(ctx); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if len(progress.seen) != 2 || !progress.seen[1].Equal(second) {
		t.Fatalf("started generations = %v, want first and second", progress.seen)
	}
}

func TestEndErrorAtGenerationBoundary(t *testing.T) {
	checkpointFailure := errors.New("checkpoint write timed out")
	flushFailure := errors.New("output flush failed")
	tests := []struct {
		name       string
		endErr     error
		closeBatch func(*streamBatchReader, gocql.UUID)
		wantErr    error
		wantLog    string
	}{
		{
			name:       "checkpoint failure at generation switch",
			endErr:     &EndCheckpointError{Err: checkpointFailure},
			closeBatch: (*streamBatchReader).closeForNextGeneration,
			wantLog:    "final checkpoint failed for stream",
		},
		{
			name:       "unmarked checkpoint failure at generation switch",
			endErr:     checkpointFailure,
			closeBatch: (*streamBatchReader).closeForNextGeneration,
			wantErr:    checkpointFailure,
		},
		{
			name:       "output failure at generation switch",
			endErr:     flushFailure,
			closeBatch: (*streamBatchReader).closeForNextGeneration,
			wantErr:    flushFailure,
		},
		{
			name:       "wrapped checkpoint error at generation switch",
			endErr:     fmt.Errorf("cleanup also failed: %w", &EndCheckpointError{Err: checkpointFailure}),
			closeBatch: (*streamBatchReader).closeForNextGeneration,
			wantErr:    checkpointFailure,
		},
		{
			name:       "checkpoint failure on StopAt",
			endErr:     &EndCheckpointError{Err: checkpointFailure},
			closeBatch: (*streamBatchReader).close,
			wantErr:    checkpointFailure,
		},
		{
			name:   "checkpoint failure on Stop",
			endErr: &EndCheckpointError{Err: checkpointFailure},
			closeBatch: func(batch *streamBatchReader, _ gocql.UUID) {
				batch.stopNow()
			},
			wantErr: checkpointFailure,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now().Add(-time.Minute)
			var logs bytes.Buffer
			batch := newStreamBatchReader(&ReaderConfig{
				ChangeConsumerFactory: &endErrorFactory{consumer: &endErrorConsumer{err: tc.endErr}},
				ProgressManager:       noProgressManager{},
				Logger:                log.New(&logs, "", 0),
				Advanced: AdvancedReaderConfig{
					QueryTimeWindowSize:  time.Second,
					ConfidenceWindowSize: time.Second,
				},
			}, start, []StreamID{StreamID("one")}, "ks", "tbl", gocql.MinTimeUUID(start))
			batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
				return emptyIterator{}, nil
			}
			tc.closeBatch(batch, gocql.MinTimeUUID(start.Add(time.Second)))
			err := batch.run(context.Background())
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("run error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantLog != "" && !strings.Contains(logs.String(), tc.wantLog) {
				t.Fatalf("missing %q in log: %s", tc.wantLog, logs.String())
			}
		})
	}
}

func TestEndFatalErrorTakesPrecedenceOverCheckpointFailure(t *testing.T) {
	checkpointFailure := errors.New("checkpoint write timed out")
	flushFailure := errors.New("output flush failed")
	start := time.Now().Add(-time.Minute)
	for i := 0; i < 20; i++ {
		batch := newStreamBatchReader(&ReaderConfig{
			ChangeConsumerFactory: &endErrorFactory{consumers: map[string]*endErrorConsumer{
				"one": {err: &EndCheckpointError{Err: checkpointFailure}},
				"two": {err: flushFailure},
			}},
			ProgressManager: noProgressManager{},
			Logger:          noLogger{},
			Advanced: AdvancedReaderConfig{
				QueryTimeWindowSize:  time.Second,
				ConfidenceWindowSize: time.Second,
			},
		}, start, []StreamID{StreamID("one"), StreamID("two")}, "ks", "tbl", gocql.MinTimeUUID(start))
		batch.queryRangeFunc = func(_, _ gocql.UUID) (cdcIterator, error) {
			return emptyIterator{}, nil
		}
		batch.closeForNextGeneration(gocql.MinTimeUUID(start.Add(time.Second)))
		err := batch.run(context.Background())
		if !errors.Is(err, flushFailure) {
			t.Fatalf("run error = %v, want flush failure", err)
		}
	}
}

type failingFinalProgressManager struct {
	noProgressManager
	err error
}

func (pm failingFinalProgressManager) SaveProgress(context.Context, time.Time, string, StreamID, Progress) error {
	return pm.err
}

func TestSaveAndStopReturnsSaveError(t *testing.T) {
	cause := errors.New("write timed out")
	reporter := NewPeriodicProgressReporter(noLogger{}, time.Hour, &ProgressReporter{
		progressManager: failingFinalProgressManager{err: cause},
		streamID:        StreamID("one"),
	})
	reporter.Start(context.Background())
	reporter.Update(gocql.TimeUUID())
	err := reporter.SaveAndStop(context.Background())
	if err != cause {
		t.Fatalf("SaveAndStop error = %v, want original error %v", err, cause)
	}
}

func TestSaveAndStopSuccess(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update bool
	}{
		{name: "with progress", update: true},
		{name: "without progress"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := NewPeriodicProgressReporter(noLogger{}, time.Hour, &ProgressReporter{
				progressManager: failingFinalProgressManager{},
				streamID:        StreamID("one"),
			})
			reporter.Start(context.Background())
			if tc.update {
				reporter.Update(gocql.TimeUUID())
			}
			if err := reporter.SaveAndStop(context.Background()); err != nil {
				t.Fatalf("SaveAndStop error = %v, want nil", err)
			}
		})
	}
}

package scyllacdc

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// PeriodicProgressReporter is a wrapper around ProgressReporter which can be
// used to save progress in regular periods of time.
type PeriodicProgressReporter struct {
	reporter *ProgressReporter
	interval time.Duration

	refreshCh    chan struct{}
	stopCh       chan struct{}
	finishCh     chan struct{}
	mu           *sync.Mutex
	timeToReport gocql.UUID

	logger Logger
}

// NewPeriodicProgressReporter creates a new PeriodicProgressReporter with
// given report interval.
func NewPeriodicProgressReporter(logger Logger, interval time.Duration, reporter *ProgressReporter) *PeriodicProgressReporter {
	return &PeriodicProgressReporter{
		reporter: reporter,
		interval: interval,

		refreshCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		finishCh:  make(chan struct{}),
		mu:        &sync.Mutex{},

		logger: logger,
	}
}

// Start spawns an internal goroutine and starts the progress reporting loop.
func (ppr *PeriodicProgressReporter) Start(ctx context.Context) {
	// Optimization: if the reporter is nil, or is NoProgressManager,
	// then don't start the goroutine at all.
	if _, ok := ppr.reporter.progressManager.(noProgressManager); ok {
		close(ppr.finishCh)
		return
	}

	go func() {
		defer close(ppr.finishCh)
		for {
			// Wait for the duration period
			select {
			case <-time.After(ppr.interval):

			case <-ctx.Done():
				return
			case <-ppr.stopCh:
				return
			}

			// Wait for a signal to refresh
			select {
			case <-ppr.refreshCh:
				ppr.mu.Lock()
				timeToReport := ppr.timeToReport
				ppr.mu.Unlock()

				// TODO: Log errors?
				err := ppr.reporter.MarkProgress(ctx, Progress{timeToReport})
				if err != nil {
					ppr.logger.Printf("failed to save progress for %s: %s", ppr.reporter.streamID, err)
				}

			case <-ctx.Done():
				return
			case <-ppr.stopCh:
				return
			}
		}
	}()
}

// Update tells the PeriodicProgressReporter that a row has been processed.
func (ppr *PeriodicProgressReporter) Update(newTime gocql.UUID) {
	ppr.mu.Lock()
	ppr.timeToReport = newTime
	ppr.mu.Unlock()

	// Fill the channel in a non-blocking manner
	select {
	case ppr.refreshCh <- struct{}{}:
	default:
	}
}

// Stop stops inner goroutine and waits until it finishes.
func (ppr *PeriodicProgressReporter) Stop() {
	close(ppr.stopCh)
	<-ppr.finishCh
}

// SaveAndStop stops inner goroutine, waits until it finishes, and then
// saves the most recent progress.
func (ppr *PeriodicProgressReporter) SaveAndStop(ctx context.Context) error {
	close(ppr.stopCh)
	<-ppr.finishCh

	// No need to lock the mutex for timeToReport
	if (ppr.timeToReport == gocql.UUID{}) {
		return nil
	}

	err := ppr.reporter.MarkProgress(ctx, Progress{ppr.timeToReport})
	if err != nil {
		ppr.logger.Printf("failed to save progress for %s: %s", ppr.reporter.streamID, err)
	} else {
		ppr.logger.Printf("successfully saved final progress for %s: %s (%s)", ppr.reporter.streamID, ppr.timeToReport, ppr.timeToReport.Time())
	}
	return err
}

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
		d := int(int8(u1[i])) - int(int8(u2[i]))
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

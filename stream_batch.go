package scylla_cdc

import (
	"context"
	"encoding/base64"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/semaphore"
)

type streamBatchReader struct {
	config         *ReaderConfig
	generationTime time.Time
	streams        []StreamID
	keyspaceName   string
	tableName      string

	lastTimestamp gocql.UUID
	endTimestamp  atomic.Value

	perStreamProgress map[string]gocql.UUID

	saveLimiter *semaphore.Weighted

	interruptCh chan struct{}
}

func newStreamBatchReader(
	config *ReaderConfig,
	generationTime time.Time,
	streams []StreamID,
	keyspaceName string,
	tableName string,
	startFrom gocql.UUID,
	saveRateLimiter *semaphore.Weighted,
) *streamBatchReader {
	return &streamBatchReader{
		config:         config,
		generationTime: generationTime,
		streams:        streams,
		keyspaceName:   keyspaceName,
		tableName:      tableName,

		lastTimestamp: startFrom,

		saveLimiter: saveRateLimiter,

		perStreamProgress: make(map[string]gocql.UUID, len(streams)),

		interruptCh: make(chan struct{}, 1),
	}
}

func (sbr *streamBatchReader) run(ctx context.Context) error {
	if err := sbr.loadProgressForStreams(); err != nil {
		return err
	}

	// sbr.config.Logger.Printf("running batch starting from %v", sbr.lastTimestamp.Time())

	input := CreateChangeConsumerInput{
		TableName: sbr.getBaseTableName(),
		StreamIDs: sbr.streams,
	}
	consumer, err := sbr.config.ChangeConsumerFactory.CreateChangeConsumer(input)
	if err != nil {
		sbr.config.Logger.Printf("error while creating change consumer (will quit): %s", err)
	} else {
		defer consumer.End()
	}

	crq := newChangeRowQuerier(sbr.config.Session, sbr.streams, sbr.keyspaceName, sbr.tableName, sbr.config.Consistency)

	// It's possible that, when starting, we are in a long period in which nothing happens.
	// Try to fast forward in the beginning
	sbr.fastForward(crq)

	wnd := sbr.getPollWindow()

	// sbr.config.Logger.Printf("starting stream processor loop for %v", sbr.streams)

outer:
	for {
		var err error
		var hadRows bool

		if compareTimeuuid(wnd.begin, wnd.end) < 0 {
			var iter *changeRowIterator
			iter, err = crq.queryRange(wnd.begin, wnd.end)
			if err != nil {
				sbr.config.Logger.Printf("error while sending a query (will retry): %s", err)
			} else {
				rowCount, consumerErr := sbr.processRows(iter, consumer)
				if err = iter.Close(); err != nil {
					sbr.config.Logger.Printf("error while querying (will retry): %s", err)
				}
				if consumerErr != nil {
					return consumerErr
				}
				hadRows = rowCount > 0
			}

			if err == nil {
				// If there were no errors, then we can safely advance
				// all streams to the window end
				sbr.advanceAllStreamsTo(wnd.end)

				if !hadRows && !wnd.touchesConfidenceWindow {
					// We had an empty poll, and it didn't touch the confidence window.
					// This means that we are probably still replaying old changes,
					// and there is a possibility that we encountered a long period
					// in which there were no changes  (for example, ChangeAgeLimit
					// was set to 1 day, but we started writing to CDC log one hour ago).
					//
					// Look up the next row for each stream in range (now, confidence
					// window end]. In next poll, we will start reading from the point
					// that those rows appear
					sbr.fastForward(crq)
				}
			}
		}

		wnd = sbr.getPollWindow()

		var delay time.Duration
		if err != nil {
			delay = sbr.config.Advanced.PostFailedQueryDelay
		} else if hadRows {
			delay = sbr.config.Advanced.PostNonEmptyQueryDelay
		} else {
			delay = sbr.config.Advanced.PostEmptyQueryDelay
		}

		delayUntil := time.Now().Add(delay)

		if sbr.reachedEndOfTheGeneration(wnd.begin) {
			break outer
		}

	delay:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delayUntil.Sub(time.Now())):
				break delay
			case <-sbr.interruptCh:
				if sbr.reachedEndOfTheGeneration(wnd.begin) {
					break outer
				}
			}
		}
	}

	sbr.saveLimiter.Acquire(ctx, 1)
	defer sbr.saveLimiter.Release(1)
	baseTableName := sbr.getBaseTableName()
	for _, stream := range sbr.streams {
		if err := sbr.config.ProgressManager.SaveProgress(sbr.generationTime, baseTableName, stream, Progress{sbr.lastTimestamp}); err != nil {
			// TODO: Should this be a hard error?
			sbr.config.Logger.Printf("error while trying to save progress for table %s, stream %v: %s", baseTableName, stream, err)
		} else {
			sbr.config.Logger.Printf("saved progress for stream %s at %v", base64.StdEncoding.EncodeToString(stream), sbr.lastTimestamp.Time())
		}
	}

	// sbr.config.Logger.Printf("successfully finishing stream processor loop for %v", sbr.streams)
	return nil
}

func (sbr *streamBatchReader) loadProgressForStreams() error {
	for _, stream := range sbr.streams {
		progress, err := sbr.config.ProgressManager.GetProgress(sbr.generationTime, sbr.getBaseTableName(), stream)
		if err != nil {
			return err
		}
		if compareTimeuuid(sbr.lastTimestamp, progress.LastProcessedRecordTime) < 0 {
			sbr.perStreamProgress[string(stream)] = progress.LastProcessedRecordTime
		} else {
			sbr.perStreamProgress[string(stream)] = sbr.lastTimestamp
		}
	}

	return nil
}

func (sbr *streamBatchReader) advanceAllStreamsTo(point gocql.UUID) {
	for id := range sbr.perStreamProgress {
		sbr.perStreamProgress[id] = point
	}
}

func (sbr *streamBatchReader) fastForward(crq *changeRowQuerier) {
	begin := sbr.getPollWindowStart()
	end := gocql.MinTimeUUID(sbr.getConfidenceLimitPoint())

	nextRowTimePerStream, err := crq.findFirstRowsInRange(begin, end)
	if err != nil {
		sbr.config.Logger.Printf("error while trying to skip forward (will ignore): %s", err)
		return
	}

	for _, s := range sbr.streams {
		if nextRowTime, ok := nextRowTimePerStream[string(s)]; ok {
			// We don't want to skip a row with this timestamp,
			// therefore subtract a millisecond
			progress := gocql.MaxTimeUUID(nextRowTime.Time().Add(-time.Millisecond))
			if compareTimeuuid(sbr.perStreamProgress[string(s)], progress) < 0 {
				sbr.perStreamProgress[string(s)] = progress
			}
		} else {
			// No row found for this stream - we can skip
			// right to confidence window end
			sbr.perStreamProgress[string(s)] = end
		}
	}
}

type pollWindow struct {
	begin gocql.UUID
	end   gocql.UUID

	touchesConfidenceWindow bool
}

func (sbr *streamBatchReader) getPollWindow() pollWindow {
	// Left range end is the minimum of all progresses of each stream
	windowStart := sbr.getPollWindowStart()

	// Right range end is the minimum of (left range + query window size, now - confidence window size)
	queryWindowRightEnd := windowStart.Time().Add(sbr.config.Advanced.QueryTimeWindowSize)
	confidenceWindowStart := sbr.getConfidenceLimitPoint()
	if queryWindowRightEnd.Before(confidenceWindowStart) {
		return pollWindow{
			begin: windowStart,
			end:   gocql.MinTimeUUID(queryWindowRightEnd),

			touchesConfidenceWindow: false,
		}
	}
	return pollWindow{
		begin: windowStart,
		end:   gocql.MinTimeUUID(confidenceWindowStart),

		touchesConfidenceWindow: true,
	}
}

func (sbr *streamBatchReader) getPollWindowStart() gocql.UUID {
	first := true
	var windowStart gocql.UUID
	for _, progress := range sbr.perStreamProgress {
		if first || compareTimeuuid(windowStart, progress) > 0 {
			windowStart = progress
		}
		first = false
	}
	return windowStart
}

func (sbr *streamBatchReader) getConfidenceLimitPoint() time.Time {
	return time.Now().Add(-sbr.config.Advanced.ConfidenceWindowSize)
}

func (sbr *streamBatchReader) processRows(iter *changeRowIterator, consumer ChangeConsumer) (int, error) {
	rowCount := 0
	var change Change

	for {
		changeBatchCols, c := iter.Next()
		if c == nil {
			break
		}
		if c.GetOperation() == PreImage {
			change.Preimage = append(change.Preimage, c)
		} else if c.GetOperation() == PostImage {
			change.Postimage = append(change.Postimage, c)
		} else {
			change.Delta = append(change.Delta, c)
		}

		rowCount++

		if c.cdcCols.endOfBatch {
			// Since we are reading in batches and we started from the lowest progress mark
			// of all streams in the batch, we might have to manually filter out changes
			// from streams that had a save point later than the earliest progress mark
			if compareTimeuuid(sbr.perStreamProgress[string(change.StreamID)], changeBatchCols.time) < 0 {
				change.StreamID = changeBatchCols.streamID
				change.Time = changeBatchCols.time
				if err := consumer.Consume(change); err != nil {
					sbr.config.Logger.Printf("error while processing change (will quit): %s", err)
					return 0, err
				}

				// It's important to save progress here. If fetching of a page fails,
				// we will have to poll again, and filter out some rows.
				sbr.perStreamProgress[string(change.StreamID)] = changeBatchCols.time
			} else {
				// sbr.config.Logger.Printf("skipping change due to it being too old (stream %v, %v <= %v)",
				// 	changeBatchCols.streamID, changeBatchCols.time, sbr.perStreamProgress[string(change.StreamID)])
			}

			change.Preimage = nil
			change.Delta = nil
			change.Postimage = nil
		}
	}

	return rowCount, nil
}

func (sbr *streamBatchReader) getBaseTableName() string {
	return sbr.keyspaceName + "." + sbr.tableName
}

func (sbr *streamBatchReader) reachedEndOfTheGeneration(windowEnd gocql.UUID) bool {
	end, isClosed := sbr.endTimestamp.Load().(gocql.UUID)
	return isClosed && (end == gocql.UUID{} || compareTimeuuid(end, windowEnd) <= 0)
}

// Only one of `close`, `stopNow` methods should be called, only once

func (sbr *streamBatchReader) close(processUntil gocql.UUID) {
	sbr.endTimestamp.Store(processUntil)
	sbr.interruptCh <- struct{}{}
}

func (sbr *streamBatchReader) stopNow() {
	sbr.close(gocql.UUID{})
}

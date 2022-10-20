package scyllacdc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type streamBatchReader struct {
	config         *ReaderConfig
	generationTime time.Time
	streams        []StreamID
	keyspaceName   string
	tableName      string

	lastTimestamp gocql.UUID
	endTimestamp  atomic.Value

	consumers map[string]ChangeConsumer

	perStreamProgress map[string]gocql.UUID

	interruptCh chan struct{}
}

func newStreamBatchReader(
	config *ReaderConfig,
	generationTime time.Time,
	streams []StreamID,
	keyspaceName string,
	tableName string,
	startFrom gocql.UUID,
) *streamBatchReader {
	return &streamBatchReader{
		config:         config,
		generationTime: generationTime,
		streams:        streams,
		keyspaceName:   keyspaceName,
		tableName:      tableName,

		lastTimestamp: startFrom,

		consumers: make(map[string]ChangeConsumer),

		perStreamProgress: make(map[string]gocql.UUID, len(streams)),

		interruptCh: make(chan struct{}, 1),
	}
}

func (sbr *streamBatchReader) run(ctx context.Context) (err error) {
	if err := sbr.loadProgressForStreams(ctx); err != nil {
		return err
	}

	defer func(err *error) {
		for s, c := range sbr.consumers {
			err2 := c.End()
			if err2 != nil {
				sbr.config.Logger.Printf("error while ending consumer for stream %s (will quit): %s", StreamID(s), err2)
			}
			if *err == nil {
				*err = err2
			}
		}
	}(&err)

	for _, s := range sbr.streams {
		input := CreateChangeConsumerInput{
			TableName: sbr.getBaseTableName(),
			StreamID:  s,

			ProgressReporter: &ProgressReporter{
				progressManager: sbr.config.ProgressManager,
				gen:             sbr.generationTime,
				tableName:       sbr.getBaseTableName(),
				streamID:        s,
			},
		}
		consumer, err := sbr.config.ChangeConsumerFactory.CreateChangeConsumer(ctx, input)
		if err != nil {
			sbr.config.Logger.Printf("error while creating change consumer (will quit): %s", err)
		}

		sbr.consumers[string(s)] = consumer
	}

	crq := newChangeRowQuerier(sbr.config.Session, sbr.streams, sbr.keyspaceName, sbr.tableName, sbr.config.Consistency)

	wnd := sbr.getPollWindow()

outer:
	for {
		var err error
		var hadRows bool

		windowProcessingStartTime := time.Now()

		if compareTimeuuid(wnd.begin, wnd.end) < 0 {
			var iter *changeRowIterator
			//sbr.config.Logger.Printf("queryRange: %s.%s :: %s (%s) [%s ... %s]",
			//	crq.keyspaceName, crq.tableName, crq.pkCondition, crq.bindArgs[0], wnd.begin.Time(), wnd.end.Time())
			iter, err = crq.queryRange(wnd.begin, wnd.end)
			if err != nil {
				sbr.config.Logger.Printf("error while sending a query (will retry): %s", err)
			} else {
				rowCount, consumerErr := sbr.processRows(ctx, iter)
				if err = iter.Close(); err != nil {
					sbr.config.Logger.Printf("error while querying (will retry): %s", err)
				}
				if consumerErr != nil {
					return consumerErr
				}
				hadRows = rowCount > 0

				if !hadRows {
					for _, c := range sbr.consumers {
						err = c.Empty(ctx, wnd.end)
					}
				}
			}

			if err == nil {
				// If there were no errors, then we can safely advance
				// all streams to the window end
				sbr.advanceAllStreamsTo(wnd.end)

				if !hadRows {
					for _, c := range sbr.consumers {
						if enc, ok := c.(ChangeOrEmptyNotificationConsumer); ok {
							err = enc.Empty(ctx, wnd.end)
						}
					}
				}
			}
		}

		wnd = sbr.getPollWindow()

		var delay time.Duration
		if err != nil {
			delay = sbr.config.Advanced.PostFailedQueryDelay
		} else {
			delay = sbr.config.Advanced.PostQueryDelay
		}

		delayUntil := windowProcessingStartTime.Add(delay)
		if time.Until(delayUntil) < time.Duration(0) {
			sbr.config.Logger.Printf("the stream can't keep up! the next poll was supposed to happen %v ago", -time.Until(delayUntil))
		}

		if sbr.reachedEndOfTheGeneration(wnd.begin) {
			break outer
		}

	delay:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Until(delayUntil)):
				break delay
			case <-sbr.interruptCh:
				if sbr.reachedEndOfTheGeneration(wnd.begin) {
					break outer
				}
			}
		}
	}

	sbr.config.Logger.Printf("ending stream batch %v", sbr.streams)

	return nil
}

func (sbr *streamBatchReader) loadProgressForStreams(ctx context.Context) error {
	for _, stream := range sbr.streams {
		progress, err := sbr.config.ProgressManager.GetProgress(ctx, sbr.generationTime, sbr.getBaseTableName(), stream)
		if err != nil {
			return err
		}
		if compareTimeuuid(sbr.lastTimestamp, progress.LastProcessedRecordTime) < 0 {
			sbr.config.Logger.Printf("loaded progress for stream %s: %s (%s)\n", stream, progress.LastProcessedRecordTime, progress.LastProcessedRecordTime.Time())
			sbr.perStreamProgress[string(stream)] = progress.LastProcessedRecordTime
		} else {
			sbr.perStreamProgress[string(stream)] = sbr.lastTimestamp
		}
	}

	return nil
}

func (sbr *streamBatchReader) advanceAllStreamsTo(point gocql.UUID) {
	for id := range sbr.perStreamProgress {
		if compareTimeuuid(sbr.perStreamProgress[id], point) < 0 {
			sbr.perStreamProgress[id] = point
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
			end:   gocql.MinTimeUUID(queryWindowRightEnd.Add(sbr.config.Advanced.PostQueryDelay)),

			touchesConfidenceWindow: false,
		}
	}
	return pollWindow{
		begin: windowStart,
		end:   gocql.MinTimeUUID(confidenceWindowStart.Add(sbr.config.Advanced.PostQueryDelay)),

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

func (sbr *streamBatchReader) processRows(ctx context.Context, iter *changeRowIterator) (int, error) {
	rowCount := 0
	var change Change

	for {
		changeBatchCols, c := iter.Next()
		if c == nil {
			break
		}
		if c.GetOperation() == PreImage {
			change.PreImage = append(change.PreImage, c)
		} else if c.GetOperation() == PostImage {
			change.PostImage = append(change.PostImage, c)
		} else {
			change.Delta = append(change.Delta, c)
		}

		rowCount++

		if c.cdcCols.endOfBatch {
			// Since we are reading in batches and we started from the lowest progress mark
			// of all streams in the batch, we might have to manually filter out changes
			// from streams that had a save point later than the earliest progress mark
			if compareTimeuuid(sbr.perStreamProgress[string(changeBatchCols.streamID)], changeBatchCols.time) < 0 {
				change.StreamID = changeBatchCols.streamID
				change.Time = changeBatchCols.time
				consumer := sbr.consumers[string(changeBatchCols.streamID)]
				if err := consumer.Consume(ctx, change); err != nil {
					sbr.config.Logger.Printf("error while processing change (will quit): %s", err)
					return 0, err
				}

				// It's important to save progress here. If fetching of a page fails,
				// we will have to poll again, and filter out some rows.
				sbr.perStreamProgress[string(changeBatchCols.streamID)] = changeBatchCols.time
			}

			change.PreImage = nil
			change.Delta = nil
			change.PostImage = nil
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

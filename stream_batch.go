package scylla_cdc

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type streamBatchReader struct {
	config        *ReaderConfig
	streams       []stream
	baseTableName string

	lastTimestamp gocql.UUID
	endTimestamp  atomic.Value

	interruptCh chan struct{}
}

func newStreamBatchReader(
	config *ReaderConfig,
	streams []stream,
	baseTableName string,
	startFrom gocql.UUID,
) *streamBatchReader {
	return &streamBatchReader{
		config:        config,
		streams:       streams,
		baseTableName: baseTableName,

		lastTimestamp: startFrom,

		interruptCh: make(chan struct{}, 1),
	}
}

func (sbr *streamBatchReader) run(ctx context.Context) (gocql.UUID, error) {
	// Prepare the primary key condition
	var pkCondition string
	if len(sbr.streams) == 1 {
		pkCondition = "\"cdc$stream_id\" = ?"
	} else {
		pkCondition = "\"cdc$stream_id\" IN (?" + strings.Repeat(", ?", len(sbr.streams)-1) + ")"
	}

	// Set up the query
	logTableName := sbr.baseTableName + cdcTableSuffix
	queryString := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s AND \"cdc$time\" > ? AND \"cdc$time\" < ? BYPASS CACHE",
		logTableName, // TODO: Sanitize table name
		pkCondition,
	)
	q := sbr.config.Session.Query(queryString)

	// Prepare a slice with bind arguments (most of them will be stream IDs)
	bindArgs := make([]interface{}, len(sbr.streams)+2)
	for i, stream := range sbr.streams {
		bindArgs[i] = stream
	}

outer:
	for {
		timeWindowEnd := sbr.lastTimestamp.Time().Add(sbr.config.Advanced.QueryTimeWindowSize)
		confidenceWindowEnd := time.Now().Add(-sbr.config.Advanced.ConfidenceWindowSize)

		if timeWindowEnd.After(confidenceWindowEnd) {
			timeWindowEnd = confidenceWindowEnd
		}

		pollEnd := gocql.MinTimeUUID(timeWindowEnd)

		var (
			err     error
			hadRows bool
		)

		readUpTo := pollEnd

		if CompareTimeuuid(sbr.lastTimestamp, pollEnd) < 0 {
			// Set the time interval from which we need to return data
			bindArgs[len(bindArgs)-2] = sbr.lastTimestamp
			bindArgs[len(bindArgs)-1] = pollEnd
			iter, err := newChangeRowIterator(q.Bind(bindArgs...).Iter())
			if err != nil {
				sbr.config.Logger.Printf("error while quering: %s", err)
				return sbr.lastTimestamp, err
			}

			var change Change
			for {
				streamCols, c := iter.Next()
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

				if c.cdcCols.endOfBatch {
					change.StreamID = streamCols.streamID
					change.Time = streamCols.time
					sbr.config.ChangeConsumer.Consume(sbr.baseTableName, change)

					change.Preimage = nil
					change.Delta = nil
					change.Postimage = nil

					// Update the last timestamp only after we processed whole batch
					if CompareTimeuuid(sbr.lastTimestamp, streamCols.time) < 0 {
						sbr.lastTimestamp = streamCols.time
					}
				}

				hadRows = true
			}

			if err = iter.Close(); err != nil {
				return sbr.lastTimestamp, err
			}
		} else {
			readUpTo = sbr.lastTimestamp
		}

		if sbr.reachedEndOfTheGeneration(readUpTo) {
			break outer
		}

		var delay time.Duration
		if err != nil {
			delay = sbr.config.Advanced.PostFailedQueryDelay
		} else if hadRows {
			delay = sbr.config.Advanced.PostNonEmptyQueryDelay
		} else {
			delay = sbr.config.Advanced.PostEmptyQueryDelay
		}

		delayUntil := time.Now().Add(delay)

	delay:
		for {
			select {
			case <-ctx.Done():
				return sbr.lastTimestamp, ctx.Err()
			case <-time.After(delayUntil.Sub(time.Now())):
				break delay
			case <-sbr.interruptCh:
				if sbr.reachedEndOfTheGeneration(readUpTo) {
					break outer
				}
			}
		}

	}
	return sbr.lastTimestamp, nil
}

func (sbr *streamBatchReader) reachedEndOfTheGeneration(windowEnd gocql.UUID) bool {
	end, isClosed := sbr.endTimestamp.Load().(gocql.UUID)
	return isClosed && (end == gocql.UUID{} || CompareTimeuuid(end, windowEnd) <= 0)
}

// Only one of `close`, `stopNow` methods should be called, only once

func (sbr *streamBatchReader) close(processUntil gocql.UUID) {
	sbr.endTimestamp.Store(processUntil)
	sbr.interruptCh <- struct{}{}
}

func (sbr *streamBatchReader) stopNow() {
	sbr.close(gocql.UUID{})
}

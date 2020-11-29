package scylla_cdc

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

const (
	// TODO: Change into parametrs
	confidenceWindow       = 3 * time.Second
	postNonEmptyQueryDelay = 1 * time.Second
	postEmptyQueryDelay    = 3 * time.Second
)

type streamBatchReader struct {
	session   *gocql.Session
	streams   []stream
	tableName string
	consumer  ChangeConsumer

	lastTimestamp gocql.UUID
	endTimestamp  atomic.Value

	interruptCh chan struct{}
}

func newStreamBatchReader(
	session *gocql.Session,
	streams []stream,
	tableName string,
	consumer ChangeConsumer,
	startFrom gocql.UUID,
) *streamBatchReader {
	return &streamBatchReader{
		session:   session,
		streams:   streams,
		tableName: tableName,
		consumer:  consumer,

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
	queryString := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s AND \"cdc$time\" > ? AND \"cdc$time\" < ? BYPASS CACHE",
		sbr.tableName, // TODO: Sanitize table name
		pkCondition,
	)
	q := sbr.session.Query(queryString)

	// Prepare a slice with bind arguments (most of them will be stream IDs)
	bindArgs := make([]interface{}, len(sbr.streams)+2)
	for i, stream := range sbr.streams {
		bindArgs[i] = stream
	}

outer:
	for {
		confidenceWindowEnd := gocql.UUIDFromTime(time.Now().Add(-confidenceWindow))
		if sbr.reachedEndOfTheGeneration(confidenceWindowEnd) {
			break outer
		}

		hadRows := false
		if CompareTimeuuid(sbr.lastTimestamp, confidenceWindowEnd) < 0 {
			// Set the time interval from which we need to return data
			bindArgs[len(bindArgs)-2] = sbr.lastTimestamp
			bindArgs[len(bindArgs)-1] = confidenceWindowEnd
			iter, err := newChangeRowIterator(q.Bind(bindArgs...).Iter())
			if err != nil {
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
					sbr.consumer.Consume(change)

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

			if err := iter.Close(); err != nil {
				return sbr.lastTimestamp, err
			}
		}

		delay := postNonEmptyQueryDelay
		if !hadRows {
			delay = postEmptyQueryDelay
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
				if sbr.reachedEndOfTheGeneration(confidenceWindowEnd) {
					break delay
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

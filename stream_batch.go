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
		hadRows := false

		if compareTimeuuid(sbr.lastTimestamp, confidenceWindowEnd) < 0 {
			// Set the time interval from which we need to return data
			bindArgs[len(bindArgs)-2] = sbr.lastTimestamp
			bindArgs[len(bindArgs)-1] = confidenceWindowEnd
			iter := q.Bind(bindArgs...).Iter()

			for {
				timestamp := gocql.UUID{}
				data := map[string]interface{}{
					"cdc$time": &timestamp,
				}
				if !iter.MapScan(data) {
					break
				}
				hadRows = true

				sbr.consumer.Consume(Change{data: data})

				if compareTimeuuid(sbr.lastTimestamp, timestamp) < 0 {
					sbr.lastTimestamp = timestamp
				}
			}

			if err := iter.Close(); err != nil {
				return sbr.lastTimestamp, nil
			}
		}

		if sbr.reachedEndOfTheGeneration(confidenceWindowEnd) {
			break outer
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
					// We need to poll at least once more
					break delay
				}
			}
		}

	}
	return sbr.lastTimestamp, nil
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

package scylla_cdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

type ReaderConfig struct {
	// An active gocql session to the cluster.
	Session *gocql.Session

	// Consistency to use when querying CDC log
	Consistency gocql.Consistency

	// Context can be used for early cancellation
	Context context.Context

	// The name of the cdc log table name. Must have _scylla_cdc_log suffix.
	// Can be prefixed with keyspace name.
	LogTableName string

	// Determines by how much to offset time lower bound of the query when reading from CDC log.
	// For more information about this option, see README.
	LowerBoundReadOffset time.Duration

	// When polling from CDC log, only rows with timestamps lower than Now - UpperBoundReadOffset are requested.
	// For more information about this option, see README.
	UpperBoundReadOffset time.Duration

	// An object which tracks cluster metadata such as current tokens and node count.
	// While this object is optional, it is highly recommended that this field points to a valid ClusterStateTracker.
	// For usage, refer to the example application.
	ClusterStateTracker *ClusterStateTracker

	// A callback which processes information fetched from the CDC log.
	ChangeConsumer ChangeConsumer
}

var (
	ErrNotALogTable = errors.New("the table is not a CDC log table")
)

const (
	cdcTableSuffix string = "_scylla_cdc_log"
)

type Reader struct {
	config  *ReaderConfig
	stopped int32
}

// Creates a new CDC reader.
func NewReader(config *ReaderConfig) (*Reader, error) {
	if config.Context == nil {
		config.Context = context.Background()
	}
	if !strings.HasSuffix(config.LogTableName, cdcTableSuffix) {
		return nil, ErrNotALogTable
	}

	reader := &Reader{
		config:  &*config, // make a copy
		stopped: 0,
	}
	return reader, nil
}

// Run runs the CDC reader. This call is blocking and returns after an error occurs, or the reader
// is stopped gracefully.
func (r *Reader) Run() error {
	gen, err := r.findLatestGeneration()
	if err != nil {
		return err
	}

	split, err := r.splitStreams(gen.streams)
	if err != nil {
		return err
	}

	errG, ctx := errgroup.WithContext(r.config.Context)
	for _, _group := range split {
		group := _group
		errG.Go(func() error {
			return r.processStreamGroup(ctx, group)
		})
	}

	return errG.Wait()
}

// Stop gracefully stops the CDC reader. It does not wait until the reader shuts down.
func (r *Reader) Stop() {
	atomic.StoreInt32(&r.stopped, 1)
}

func (r *Reader) splitStreams(streams []stream) (split [][]stream, err error) {
	if r.config.ClusterStateTracker == nil {
		// This method of polling is quite bad for performance, but we cannot do better without the tracker
		// TODO: Maybe forbid, or at least warn?
		for _, s := range streams {
			split = append(split, []stream{s})
		}
		return
	}

	tokens := r.config.ClusterStateTracker.GetTokens()
	vnodesToStreams := make(map[int64][]stream, 0)

	for _, stream := range streams {
		var streamInt int64
		if err := binary.Read(bytes.NewReader(stream), binary.BigEndian, &streamInt); err != nil {
			return nil, err
		}
		idx := sort.Search(len(tokens), func(i int) bool {
			return !(tokens[i] < streamInt)
		})
		if idx >= len(tokens) {
			idx = 0
		}
		chosenTok := tokens[idx]
		vnodesToStreams[chosenTok] = append(vnodesToStreams[chosenTok], stream)
	}

	groups := make([][]stream, 0, len(vnodesToStreams))
	for _, streams := range vnodesToStreams {
		groups = append(groups, streams)
	}
	return groups, nil
}

func (r *Reader) processStreamGroup(ctx context.Context, streams []stream) error {
	if len(streams) == 0 {
		return nil
	}

	lastTimestamp := gocql.MinTimeUUID(time.Now())

	streamsWildcards := "?" + strings.Repeat(", ?", len(streams)-1)
	queryString := fmt.Sprintf(
		"SELECT * FROM %s WHERE \"cdc$stream_id\" IN (%s) AND \"cdc$time\" > ? AND \"cdc$time\" < ?",
		r.config.LogTableName,
		streamsWildcards,
	)
	query := r.config.Session.Query(queryString).Consistency(r.config.Consistency)

	for {
		if atomic.LoadInt32(&r.stopped) != 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		readRangeStartTimestamp := lastTimestamp
		if r.config.LowerBoundReadOffset != 0 {
			readRangeStartTimestamp = gocql.MinTimeUUID(lastTimestamp.Time().Add(-r.config.LowerBoundReadOffset))
		}

		readRangeEndTimestamp := gocql.MaxTimeUUID(time.Now().Add(-r.config.UpperBoundReadOffset))

		bindArgs := make([]interface{}, 0, len(streams)+2)
		for _, stream := range streams {
			bindArgs = append(bindArgs, stream)
		}
		bindArgs = append(bindArgs, readRangeStartTimestamp, readRangeEndTimestamp)

		iter := query.Bind(bindArgs...).Iter()

		for {
			timestamp := &gocql.UUID{}
			data := map[string]interface{}{
				"cdc$time": timestamp,
			}
			if !iter.MapScan(data) {
				break
			}

			r.config.ChangeConsumer.Consume(Change{data: data})

			if bytes.Compare(lastTimestamp[:], timestamp[:]) < 0 {
				lastTimestamp = *timestamp
			}
		}

		if err := iter.Close(); err != nil {
			return err
		}
	}
}

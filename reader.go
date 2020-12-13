package scylla_cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type ReaderConfig struct {
	// An active gocql session to the cluster.
	Session *gocql.Session

	// Names of the tables for which to read changes. This should be the name
	// of the base table, not the cdc log table.
	// Can be prefixed with keyspace name.
	TableNames []string

	// Consistency to use when querying CDC log
	Consistency gocql.Consistency

	// Creates ChangeProcessors, which process information fetched from the CDC log.
	// A callback which processes information fetched from the CDC log.
	ChangeConsumerFactory ChangeConsumerFactory

	// An object which allows the reader to read and write information about
	// current progress.
	ProgressManager ProgressManager

	// A logger. If set, it will receive log messages useful for debugging of the library.
	Logger Logger

	// Advanced parameters
	Advanced AdvancedReaderConfig
}

// TODO: Document
type AdvancedReaderConfig struct {
	ConfidenceWindowSize time.Duration

	PostNonEmptyQueryDelay time.Duration
	PostEmptyQueryDelay    time.Duration
	PostFailedQueryDelay   time.Duration

	QueryTimeWindowSize time.Duration
	ChangeAgeLimit      time.Duration
}

// Creates a ReaderConfig struct with safe defaults.
func NewReaderConfig(
	session *gocql.Session,
	consumerFactory ChangeConsumerFactory,
	progressManager ProgressManager,
	tableNames ...string,
) *ReaderConfig {
	return &ReaderConfig{
		Session:               session,
		TableNames:            tableNames,
		Consistency:           gocql.Quorum,
		ChangeConsumerFactory: consumerFactory,
		ProgressManager:       progressManager,

		Advanced: AdvancedReaderConfig{
			ConfidenceWindowSize: 30 * time.Second,

			PostNonEmptyQueryDelay: 10 * time.Second,
			PostEmptyQueryDelay:    30 * time.Second,
			PostFailedQueryDelay:   1 * time.Second,

			QueryTimeWindowSize: 30 * time.Second,
			ChangeAgeLimit:      1 * time.Minute, // TODO: Does that make sense?
		},
	}
}

func (rc *ReaderConfig) Copy() *ReaderConfig {
	newRC := &ReaderConfig{}
	*newRC = *rc
	return newRC
}

var (
	ErrNotALogTable = errors.New("the table is not a CDC log table")
)

const (
	cdcTableSuffix string = "_scylla_cdc_log"
)

type Reader struct {
	config     *ReaderConfig
	genFetcher *generationFetcher
	readFrom   time.Time
	stoppedCh  chan struct{}
	stopTime   atomic.Value

	saveLimiter *semaphore.Weighted
}

// Creates a new CDC reader.
func NewReader(config *ReaderConfig) (*Reader, error) {
	if config.Logger == nil {
		config.Logger = &noLogger{}
	}

	if config.ProgressManager == nil {
		return nil, errors.New("no progress manager was specified")
	}

	readFrom, err := config.ProgressManager.GetCurrentGeneration()
	if err != nil {
		return nil, err
	}
	if readFrom.IsZero() {
		readFrom = time.Now().Add(-config.Advanced.ChangeAgeLimit)
		config.Logger.Printf("no saved progress found, will start reading from %v", readFrom)
	} else {
		config.Logger.Printf("last saved progress was at generation %v", readFrom)
	}

	genFetcher, err := newGenerationFetcher(
		config.Session,
		readFrom,
		config.Logger,
	)
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		config:     config.Copy(),
		genFetcher: genFetcher,
		readFrom:   readFrom,
		stoppedCh:  make(chan struct{}),

		saveLimiter: semaphore.NewWeighted(100),
	}
	return reader, nil
}

// Run runs the CDC reader. This call is blocking and returns after an error occurs, or the reader
// is stopped gracefully.
func (r *Reader) Run(ctx context.Context) error {
	l := r.config.Logger

	runErrG, runCtx := errgroup.WithContext(ctx)

	runErrG.Go(func() error {
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-r.stoppedCh:
		}
		r.genFetcher.Stop()
		return nil
	})
	runErrG.Go(func() error {
		return r.genFetcher.Run(runCtx)
	})
	runErrG.Go(func() error {
		gen, err := r.genFetcher.Get(runCtx)
		if gen == nil {
			return err
		}

		if r.readFrom.Before(gen.startTime) {
			r.readFrom = gen.startTime
		}

		for {
			l.Printf("starting reading generation %v from timestamp %v", gen.startTime, r.readFrom)

			if err := r.config.ProgressManager.StartGeneration(gen.startTime); err != nil {
				return err
			}

			// Start batch readers for this generation
			split := r.splitStreams(gen.streams)

			l.Printf("grouped %d streams into %d batches", len(gen.streams), len(split))

			genErrG, genCtx := errgroup.WithContext(runCtx)

			readers := make([]*streamBatchReader, 0, len(split)*len(r.config.TableNames))
			for _, tableName := range r.config.TableNames {
				// TODO: This is ugly?
				splitName := strings.SplitN(tableName, ".", 2)
				for _, group := range split {
					readers = append(readers, newStreamBatchReader(
						r.config,
						gen.startTime,
						group,
						splitName[0],
						splitName[1],
						gocql.MinTimeUUID(r.readFrom),
						r.saveLimiter,
					))
				}
			}

			sleepAmount := r.config.Advanced.PostNonEmptyQueryDelay / time.Duration(len(readers))
			for i := range readers {
				reader := readers[i]
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleepAmount):
				}
				genErrG.Go(func() error {
					return reader.run(genCtx)
				})
			}

			var nextGen *generation
			genErrG.Go(func() error {
				var err error
				nextGen, err = r.genFetcher.Get(genCtx)
				if err != nil {
					return err
				}
				for _, reader := range readers {
					if nextGen == nil {
						// The reader was stopped
						stopAt, _ := r.stopTime.Load().(time.Time)
						if stopAt.IsZero() {
							reader.stopNow()
						} else {
							reader.close(gocql.MaxTimeUUID(stopAt))
							r.readFrom = stopAt
						}
					} else {
						reader.close(gocql.MinTimeUUID(nextGen.startTime))
						r.readFrom = nextGen.startTime
					}
				}
				return nil
			})

			if err := genErrG.Wait(); err != nil {
				return err
			}
			l.Printf("stopped reading from generation %v", gen.startTime)
			if nextGen == nil {
				break
			}
			gen = nextGen
		}

		return nil
	})

	return runErrG.Wait()
}

// Stop tells the reader to stop as soon as possible. There is no guarantee
// related to how much data will be processed in each stream when the reader
// stops. If you want to e.g. make sure that all cdc log data with timestamps
// up to the current moment was processed, use (*Reader).StopAt(time.Now()).
// This function does not wait until the reader stops.
func (r *Reader) Stop() {
	close(r.stoppedCh)
}

// StopAt tells the reader to stop reading changes after reaching given timestamp.
// Does not guarantee that the reader won't read any changes after the timestamp,
// but the reader will stop after all tables and streams are advanced to or past
// the timestamp.
// This function does not wait until the reader stops.
func (r *Reader) StopAt(at time.Time) {
	r.stopTime.Store(at)
	close(r.stoppedCh)
}

func (r *Reader) splitStreams(streams []StreamID) [][]StreamID {
	vnodesIdxToStreams := make(map[int64][]StreamID, 0)
	for _, stream := range streams {
		idx := getVnodeIndexForStream(stream)
		vnodesIdxToStreams[idx] = append(vnodesIdxToStreams[idx], stream)
	}

	groups := make([][]StreamID, 0)

	// Idx -1 means that we don't know the vnode for given stream,
	// therefore we will put those streams into a separate group
	for _, stream := range vnodesIdxToStreams[-1] {
		groups = append(groups, []StreamID{stream})
	}
	delete(vnodesIdxToStreams, -1)

	for _, group := range vnodesIdxToStreams {
		groups = append(groups, group)
	}
	return groups
}

// Computes vnode index from given stream ID.
// Returns -1 if the stream ID format is unrecognized.
func getVnodeIndexForStream(streamID StreamID) int64 {
	if len(streamID) != 16 {
		// Don't know how to handle other sizes
		return -1
	}

	lowerQword := binary.BigEndian.Uint64(streamID[8:16])
	version := lowerQword & (1<<4 - 1)
	if version != 1 {
		// Unrecognized version
		return -1
	}

	vnodeIdx := (lowerQword >> 4) & (1<<22 - 1)
	return int64(vnodeIdx)
}

package scylla_cdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

	// An object which tracks cluster metadata such as current tokens and node count.
	// While this object is optional, it is highly recommended that this field points to a valid ClusterStateTracker.
	// For usage, refer to the example application.
	ClusterStateTracker *ClusterStateTracker

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
		config.ClusterStateTracker,
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
			split, err := r.splitStreams(gen.streams)
			if err != nil {
				return err
			}

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
					))
				}
			}

			sleepAmount := r.config.Advanced.PostNonEmptyQueryDelay / time.Duration(len(readers))
			for i := range readers {
				reader := readers[i] // TODO: Should this be interruptible?
				<-time.After(sleepAmount)
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

			if err := r.config.ProgressManager.EndGeneration(gen.startTime); err != nil {
				return err
			}
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

func (r *Reader) splitStreams(streams []StreamID) (split [][]StreamID, err error) {
	if r.config.ClusterStateTracker == nil {
		// This method of polling is quite bad for performance, but we cannot do better without the tracker
		// TODO: Maybe forbid, or at least warn?
		for _, s := range streams {
			split = append(split, []StreamID{s})
		}
		return
	}

	tokens := r.config.ClusterStateTracker.GetTokens()
	vnodesToStreams := make(map[int64][]StreamID, 0)

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

	groups := make([][]StreamID, 0, len(vnodesToStreams))
	for _, streams := range vnodesToStreams {
		groups = append(groups, streams)
	}
	return groups, nil
}

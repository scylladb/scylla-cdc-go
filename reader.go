package scylla_cdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

type ReaderConfig struct {
	// An active gocql session to the cluster.
	Session *gocql.Session

	// Names of the cdc log tables to read from. Must have _scylla_cdc_log suffix.
	// Can be prefixed with keyspace name.
	LogTableNames []string

	// Consistency to use when querying CDC log
	Consistency gocql.Consistency

	// A callback which processes information fetched from the CDC log.
	ChangeConsumer ChangeConsumer

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
	consumer ChangeConsumer,
	logTableNames ...string,
) *ReaderConfig {
	return &ReaderConfig{
		Session:        session,
		LogTableNames:  logTableNames,
		Consistency:    gocql.Quorum,
		ChangeConsumer: consumer,

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
}

// Creates a new CDC reader.
func NewReader(config *ReaderConfig) (*Reader, error) {
	if config.Logger == nil {
		config.Logger = &noLogger{}
	}

	for _, tableName := range config.LogTableNames {
		if !strings.HasSuffix(tableName, cdcTableSuffix) {
			return nil, ErrNotALogTable
		}
	}

	readFrom := time.Now().Add(-config.Advanced.ChangeAgeLimit)

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
	// TODO: Return a "snapshot" or something

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

		for {
			l.Printf("starting reading from generation %v", r.readFrom)

			// Start batch readers for this generation
			split, err := r.splitStreams(gen.streams)
			if err != nil {
				return err
			}

			l.Printf("grouped %d streams into %d batches", len(gen.streams), len(split))

			genErrG, genCtx := errgroup.WithContext(runCtx)

			readers := make([]*streamBatchReader, 0, len(split)*len(r.config.LogTableNames))
			for _, tableName := range r.config.LogTableNames {
				for _, group := range split {
					readers = append(readers, newStreamBatchReader(
						r.config,
						group,
						tableName,
						gocql.MinTimeUUID(r.readFrom),
					))
				}
			}

			for i := range readers {
				reader := readers[i]
				// TODO: slightly sleep before creating each of them
				// in order to make them more distributed in time
				genErrG.Go(func() error {
					// TODO: Do something sensible with returned timeuuid
					_, err := reader.run(genCtx)
					return err
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
						reader.stopNow()
					} else {
						reader.close(gocql.MaxTimeUUID(nextGen.startTime))
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

// Stop gracefully stops the CDC reader. It does not wait until the reader shuts down.
func (r *Reader) Stop() {
	close(r.stoppedCh)
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

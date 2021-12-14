package scyllacdc

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

// ReaderConfig defines parameters used for creation of the CDC Reader object.
type ReaderConfig struct {
	// An active gocql session to the cluster.
	Session *gocql.Session

	// Names of the tables for which to read changes. This should be the name
	// of the base table, not the cdc log table.
	// Can be prefixed with keyspace name.
	TableNames []string

	// Consistency to use when querying CDC log.
	// If not specified, QUORUM consistency will be used.
	Consistency gocql.Consistency

	// Creates ChangeProcessors, which process information fetched from the CDC log.
	// A callback which processes information fetched from the CDC log.
	ChangeConsumerFactory ChangeConsumerFactory

	// An object which allows the reader to read and write information about
	// current progress.
	ProgressManager ProgressManager

	// A logger. If set, it will receive log messages useful for debugging of the library.
	Logger Logger

	// Advanced parameters.
	Advanced AdvancedReaderConfig
}

func (rc *ReaderConfig) validate() error {
	if len(rc.TableNames) == 0 {
		return errors.New("no table names specified to read from")
	}
	if rc.ChangeConsumerFactory == nil {
		return errors.New("no change consumer factory specified")
	}

	return nil
}

func (rc *ReaderConfig) setDefaults() {
	if rc.Consistency == 0 {
		// Consistency 0 is ANY. It doesn't make sense
		// to use it for reading, so default to QUORUM instead
		rc.Consistency = gocql.Quorum
	}
	if rc.ProgressManager == nil {
		rc.ProgressManager = noProgressManager{}
	}
	if rc.Logger == nil {
		rc.Logger = noLogger{}
	}
	rc.Advanced.setDefaults()
}

// AdvancedReaderConfig contains advanced parameters that control behavior
// of the CDC Reader. It is not recommended to change them unless really
// necessary. They have carefully selected default values that should work for
// most cases. Changing these parameters need to be done carefully.
type AdvancedReaderConfig struct {
	// ConfidenceWindowSize defines a minimal age a change must have in order
	// to be read.
	//
	// Due to the eventually consistent nature of Scylla, newer writes may
	// appear in CDC log earlier than some older writes. This can cause the
	// Reader to skip the older write, therefore the need for this parameter.
	//
	// If the parameter is left as 0, the library will automatically choose
	// a default confidence window size.
	ConfidenceWindowSize time.Duration

	// The library uses select statements to fetch changes from CDC Log tables.
	// Each select fetches changes from a single table and fetches only changes
	// from a limited set of CDC streams. The subsequent select after query
	// execution to this table and set of CDC streams will be issued after
	// a delay. This parameter specifies the length of the delay.
	//
	// If the parameter is left as 0, the library will automatically adjust
	// the length of the delay.
	PostQueryDelay time.Duration

	// If the library tries to read from the CDC log and the read operation
	// fails, it will wait some time before attempting to read again. This
	// parameter specifies the length of the delay.
	//
	// If the parameter is left as 0, the library will automatically adjust
	// the length of the delay.
	PostFailedQueryDelay time.Duration

	// Changes are queried using select statements with restriction on the time
	// those changes appeared. The restriction is bounding the time from both
	// lower and upper bounds. This parameter defines the width of the time
	// window used for the restriction.
	//
	// If the parameter is left as 0, the library will automatically adjust
	// the size of the restriction window.
	QueryTimeWindowSize time.Duration

	// When the library starts for the first time it has to start consuming
	// changes from some point in time. This parameter defines how far in the
	// past it needs to look. If the value of the parameter is set to an hour,
	// then the library will only read historical changes that are no older than
	// an hour.
	//
	// Note of caution: data in CDC Log table is automatically deleted so
	// setting this parameter to something bigger than TTL used on CDC Log won’t
	// cause changes older than this TTL to appear.
	//
	// If the parameter is left as 0, the library will automatically adjust
	// the size of the restriction window.
	ChangeAgeLimit time.Duration
}

func (arc *AdvancedReaderConfig) setDefaults() {
	setIfZero := func(p *time.Duration, v time.Duration) {
		if *p == 0 {
			*p = v
		}
	}
	setIfZero(&arc.ConfidenceWindowSize, 30*time.Second)

	setIfZero(&arc.PostQueryDelay, 10*time.Second)
	setIfZero(&arc.PostFailedQueryDelay, 1*time.Second)

	setIfZero(&arc.QueryTimeWindowSize, 30*time.Second)
	setIfZero(&arc.ChangeAgeLimit, 1*time.Minute)
}

// Copy makes a shallow copy of the ReaderConfig.
func (rc *ReaderConfig) Copy() *ReaderConfig {
	newRC := &ReaderConfig{}
	*newRC = *rc
	return newRC
}

const (
	cdcTableSuffix string = "_scylla_cdc_log"
)

// Reader reads changes from CDC logs of the specified tables.
type Reader struct {
	config     *ReaderConfig
	genFetcher *generationFetcher
	readFrom   time.Time
	stoppedCh  chan struct{}
	stopTime   atomic.Value
}

// NewReader creates a new CDC reader using the specified configuration.
func NewReader(ctx context.Context, config *ReaderConfig) (*Reader, error) {
	config = config.Copy()

	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}

	readFrom, err := config.ProgressManager.GetCurrentGeneration(ctx)
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
		config:     config,
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

			if err := r.config.ProgressManager.StartGeneration(ctx, gen.startTime); err != nil {
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
					))
				}
			}

			sleepAmount := r.config.Advanced.PostQueryDelay / time.Duration(len(readers))
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

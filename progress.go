package scyllacdc

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/semaphore"
)

// ProgressManager allows the library to load and save progress for each
// stream and table separately.
type ProgressManager interface {
	// GetCurrentGeneration returns the time of the generation that was
	// last saved by StartGeneration. The library will call this function
	// at the beginning in order to determine from which generation it should
	// start reading first.
	//
	// If there is no information available about the time of the generation
	// from which reading should start, GetCurrentGeneration can return
	// a zero time value. In that case, reading will start from the point
	// determined by AdvancedReaderConfig.ChangeAgeLimit.
	//
	// If this function returns an error, the library will stop with an error.
	GetCurrentGeneration(ctx context.Context) (time.Time, error)

	// StartGeneration is called after all changes have been read from the
	// previous generation and the library is about to start processing
	// the next one. The ProgressManager should save this information so that
	// GetCurrentGeneration will return it after the library is restarted.
	//
	// If this function returns an error, the library will stop with an error.
	StartGeneration(ctx context.Context, gen time.Time) error

	// GetProgress retrieves information about the progress of given stream,
	// in a given table. If there was no progress saved for this stream
	// during this generation, GetProgress can return a zero time value
	// and the library will start processing changes from the stream
	// starting from the beginning of the generation.
	//
	// This method needs to be thread-safe, as the library is allowed to
	// call it concurrently for different combinations of `table` and `streamID`.
	// The library won't issue concurrent calls to this method with the same
	// `table` and `streamID` parameters.
	//
	// If this function returns an error, the library will stop with an error.
	GetProgress(ctx context.Context, gen time.Time, table string, streamID StreamID) (Progress, error)

	// SaveProgress stores information about the last cdc log record which was
	// processed successfully. If the reader is restarted, it should resume
	// work for this stream starting from the row _after_ the last saved
	// timestamp.
	//
	// This method is only called by ChangeConsumers, indirectly through
	// the ProgressReporter struct. Within a generation, ChangeConsumers
	// are run concurrently, therefore SaveProgress should be safe to call
	// concurrently.
	//
	// Contrary to other methods, an error returned does not immediately
	// result in the library stopping with an error. The error is propagated
	// to the ChangeConsumer, and it can decide what to do with the error next.
	SaveProgress(ctx context.Context, gen time.Time, table string, streamID StreamID, progress Progress) error
}

// ProgressManagerWithStartTime is an extension to the ProgressManager interface.
type ProgressManagerWithStartTime interface {
	ProgressManager

	// GetApplicationReadStartTime returns the timestamp from which
	// the application started reading data. The library uses this timestamp
	// as a lower bound to determine where it should start reading. For example,
	// if there is no generation saved or there is no progress information
	// saved for a stream, reading will be restarted from the given timestamp
	// (or higher if the generation timestamp is higher).
	//
	// If this function returns a zero timeuuid, the library will start reading
	// from `time.Now() - AdvancedReaderConfig.ChangeAgeLimit`.
	// If this function returns an error, the library will stop with an error.
	GetApplicationReadStartTime(ctx context.Context) (time.Time, error)

	// SaveApplicationReadStartTime stores information about the timestamp
	// from which the application originally started reading data.
	// It is called by the library if there was no start timestamp saved.
	//
	// If this function returns an error, the library will stop with an error.
	SaveApplicationReadStartTime(ctx context.Context, startTime time.Time) error
}

// ProgressReporter is a helper object for the ChangeConsumer. It allows
// the consumer to save its progress.
type ProgressReporter struct {
	progressManager ProgressManager
	gen             time.Time
	tableName       string
	streamID        StreamID
}

// MarkProgress saves progress for the consumer associated with the ProgressReporter.
//
// The associated ChangeConsumer is allowed to call it anytime between its
// creation by ChangeConsumerFactory and the moment it is stopped (the call to
// (ChangeConsumer).End() finishes).
func (pr *ProgressReporter) MarkProgress(ctx context.Context, progress Progress) error {
	return pr.progressManager.SaveProgress(ctx, pr.gen, pr.tableName, pr.streamID, progress)
}

// Progress represents the point up to which the library has processed changes
// in a given stream.
type Progress struct {
	// LastProcessedRecordTime represents the value of the cdc$time column
	// of the last processed record in the stream.
	LastProcessedRecordTime gocql.UUID
}

// noProgressManager does not actually save any progress, and always reports
// zero progress. This implementation can be used when saving progress
// is not necessary for the application.
type noProgressManager struct{}

// GetCurrentGeneration is needed to implement the ProgressManager interface.
func (noProgressManager) GetCurrentGeneration(ctx context.Context) (time.Time, error) {
	return time.Time{}, nil
}

// StartGeneration is needed to implement the ProgressManager interface.
func (noProgressManager) StartGeneration(ctx context.Context, gen time.Time) error {
	return nil
}

// GetProgress is needed to implement the ProgressManager interface.
func (noProgressManager) GetProgress(ctx context.Context, gen time.Time, table string, streamID StreamID) (Progress, error) {
	return Progress{}, nil
}

// SaveProgress is needed to implement the ProgressManager interface.
func (noProgressManager) SaveProgress(ctx context.Context, gen time.Time, table string, streamID StreamID, progress Progress) error {
	return nil
}

// TableBackedProgressManager is a ProgressManager which saves progress in a Scylla table.
//
// The schema is as follows:
//
//  CREATE TABLE IF NOT EXISTS <table name> (
//      generation timestamp,
//      application_name text,
//      table_name text,
//      stream_id blob,
//      last_timestamp timeuuid,
//      current_generation timestamp,
//      PRIMARY KEY ((generation, application_name, table_name, stream_id))
//  )
//
// Progress for each stream is stored in a separate row, indexed by generation,
// application_name, table_name and stream_id.
//
// For storing information about current generation, special rows with stream
// set to empty bytes is used.
type TableBackedProgressManager struct {
	session           *gocql.Session
	progressTableName string
	applicationName   string

	// TTL to use when writing progress for a stream (a week by default).
	// TODO: maybe not? maybe we should clean up this data manually?
	// Progress data may be large if generations are very large
	ttl int32

	concurrentQueryLimiter *semaphore.Weighted
}

// NewTableBackedProgressManager creates a new TableBackedProgressManager.
func NewTableBackedProgressManager(session *gocql.Session, progressTableName string, applicationName string) (*TableBackedProgressManager, error) {
	tbpm := &TableBackedProgressManager{
		session:           session,
		progressTableName: progressTableName,
		applicationName:   applicationName,

		ttl: 7 * 24 * 60 * 60, // 1 week

		concurrentQueryLimiter: semaphore.NewWeighted(100), // TODO: Make units configurable
	}

	if err := tbpm.ensureTableExists(); err != nil {
		return nil, err
	}
	return tbpm, nil
}

// SetTTL sets the TTL used to expire progress. By default, it's 7 days.
func (tbpm *TableBackedProgressManager) SetTTL(ttl int32) {
	tbpm.ttl = ttl
}

// SetMaxConcurrency sets the maximum allowed concurrency for write operations.
// By default, it's 100.
// This function must not be called after Reader for this manager is started.
func (tbpm *TableBackedProgressManager) SetMaxConcurrency(maxConcurrentOps int64) {
	tbpm.concurrentQueryLimiter = semaphore.NewWeighted(maxConcurrentOps)
}

func (tbpm *TableBackedProgressManager) ensureTableExists() error {
	return tbpm.session.Query(
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s "+
				"(generation timestamp, application_name text, table_name text, stream_id blob, last_timestamp timeuuid, current_generation timestamp, "+
				"PRIMARY KEY ((generation, application_name, table_name, stream_id)))",
			tbpm.progressTableName,
		),
	).Exec()
}

// GetCurrentGeneration is needed to implement the ProgressManager interface.
func (tbpm *TableBackedProgressManager) GetCurrentGeneration(ctx context.Context) (time.Time, error) {
	var gen time.Time
	err := tbpm.session.Query(
		fmt.Sprintf("SELECT current_generation FROM %s WHERE generation = ? AND application_name = ? AND table_name = ? AND stream_id = ?", tbpm.progressTableName),
		time.Time{}, tbpm.applicationName, "", []byte{},
	).Scan(&gen)

	if err != nil && err != gocql.ErrNotFound {
		return time.Time{}, err
	}
	return gen, nil
}

// StartGeneration is needed to implement the ProgressManager interface.
func (tbpm *TableBackedProgressManager) StartGeneration(ctx context.Context, gen time.Time) error {
	// Update the progress in the special partition
	return tbpm.session.Query(
		fmt.Sprintf(
			"INSERT INTO %s (generation, application_name, table_name, stream_id, current_generation) "+
				"VALUES (?, ?, ?, ?, ?)",
			tbpm.progressTableName,
		),
		time.Time{}, tbpm.applicationName, "", []byte{}, gen,
	).Exec()
}

// GetProgress is needed to implement the ProgressManager interface.
func (tbpm *TableBackedProgressManager) GetProgress(ctx context.Context, gen time.Time, tableName string, streamID StreamID) (Progress, error) {
	tbpm.concurrentQueryLimiter.Acquire(ctx, 1)
	defer tbpm.concurrentQueryLimiter.Release(1)

	var timestamp gocql.UUID
	err := tbpm.session.Query(
		fmt.Sprintf("SELECT last_timestamp FROM %s WHERE generation = ? AND application_name = ? AND table_name = ? AND stream_id = ?", tbpm.progressTableName),
		gen, tbpm.applicationName, tableName, streamID,
	).Scan(&timestamp)

	if err != nil && err != gocql.ErrNotFound {
		return Progress{}, err
	}
	return Progress{timestamp}, nil
}

// SaveProgress is needed to implement the ProgressManager interface.
func (tbpm *TableBackedProgressManager) SaveProgress(ctx context.Context, gen time.Time, tableName string, streamID StreamID, progress Progress) error {
	tbpm.concurrentQueryLimiter.Acquire(ctx, 1)
	defer tbpm.concurrentQueryLimiter.Release(1)

	//log.Printf("SaveProgress for %s = %s\n", streamID, progress.LastProcessedRecordTime)
	return tbpm.session.Query(
		fmt.Sprintf("INSERT INTO %s (generation, application_name, table_name, stream_id, last_timestamp) VALUES (?, ?, ?, ?, ?) USING TTL ?", tbpm.progressTableName),
		gen, tbpm.applicationName, tableName, streamID, progress.LastProcessedRecordTime, tbpm.ttl,
	).Exec()
}

// SaveApplicationReadStartTime is needed to implement the ProgressManagerWithStartTime interface.
func (tbpm *TableBackedProgressManager) SaveApplicationReadStartTime(ctx context.Context, startTime time.Time) error {
	// Store information about the timestamp in the `last_timestamp` column,
	// in the special partition with "zero generation".
	return tbpm.session.Query(
		fmt.Sprintf(
			"INSERT INTO %s (generation, application_name, table_name, stream_id, last_timestamp) "+
				"VALUES (?, ?, ?, ?, ?)",
			tbpm.progressTableName,
		),
		time.Time{}, tbpm.applicationName, "", []byte{}, gocql.MinTimeUUID(startTime),
	).Exec()
}

// GetApplicationReadStartTime is needed to implement the ProgressManagerWithStartTime interface.
func (tbpm *TableBackedProgressManager) GetApplicationReadStartTime(ctx context.Context) (time.Time, error) {
	// Retrieve the information from the special column
	var timestamp gocql.UUID
	err := tbpm.session.Query(
		fmt.Sprintf("SELECT last_timestamp FROM %s WHERE generation = ? AND application_name = ? AND table_name = ? AND stream_id = ?", tbpm.progressTableName),
		time.Time{}, tbpm.applicationName, "", []byte{},
	).Scan(&timestamp)
	if err != nil && err != gocql.ErrNotFound {
		return time.Time{}, err
	}
	return timestamp.Time(), nil
}

var _ ProgressManager = (*TableBackedProgressManager)(nil)
var _ ProgressManagerWithStartTime = (*TableBackedProgressManager)(nil)

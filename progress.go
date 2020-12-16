package scylla_cdc

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/semaphore"
)

type ProgressManager interface {
	// GetCurrentGenerationTime returns the time of the generation that was
	// saved as being currently processed. If there was no such information
	// saved, it returns a zero value for Time.
	GetCurrentGeneration(ctx context.Context) (time.Time, error)

	// StartGeneration is called when a generation is discovered and the reader
	// should start processing the next generation.
	StartGeneration(ctx context.Context, gen time.Time) error

	// GetProgress retrieves information about the progress of given stream,
	// in a given table. It can return zero value for Progress if there was
	// no information about the stream
	GetProgress(ctx context.Context, gen time.Time, table string, streamID StreamID) (Progress, error)

	// SaveProgress stores information about the last cdc log record which was
	// processed successfully. If the reader is restarted, it should resume
	// work for this stream from the last saved
	SaveProgress(ctx context.Context, gen time.Time, table string, streamID StreamID, progress Progress) error
}

type ProgressReporter struct {
	progressManager ProgressManager
	gen             time.Time
	tableName       string
	streamID        StreamID
}

func (pr *ProgressReporter) MarkProgress(ctx context.Context, progress Progress) error {
	return pr.progressManager.SaveProgress(ctx, pr.gen, pr.tableName, pr.streamID, progress)
}

type Progress struct {
	LastProcessedRecordTime gocql.UUID
}

// NoProgressManager does not persist the progress at all. It can be used
// when saving progress is not necessary.
type NoProgressManager struct{}

func (*NoProgressManager) GetCurrentGeneration(ctx context.Context) (time.Time, error) {
	return time.Time{}, nil
}

func (*NoProgressManager) StartGeneration(ctx context.Context, gen time.Time) error {
	return nil
}

func (*NoProgressManager) GetProgress(ctx context.Context, gen time.Time, table string, streamID StreamID) (Progress, error) {
	return Progress{}, nil
}

func (*NoProgressManager) SaveProgress(ctx context.Context, gen time.Time, table string, streamID StreamID, progress Progress) error {
	return nil
}

type TableBackedProgressManager struct {
	session           *gocql.Session
	progressTableName string

	// TTL to use when writing progress for a stream (a week by default).
	// TODO: maybe not? maybe we should clean up this data manually?
	// Progress data may be large if generations are very large
	ttl int32

	concurrentQueryLimiter *semaphore.Weighted
}

func NewTableBackedProgressManager(session *gocql.Session, progressTableName string, applicationName string) (*TableBackedProgressManager, error) {
	tbpm := &TableBackedProgressManager{
		session:           session,
		progressTableName: progressTableName,

		ttl: 7 * 24 * 60 * 60, // 1 week

		concurrentQueryLimiter: semaphore.NewWeighted(100), // TODO: Make units configurable
	}

	if err := tbpm.ensureTableExists(); err != nil {
		return nil, err
	}
	return tbpm, nil
}

func (tbpm *TableBackedProgressManager) SetTTL(ttl int32) {
	tbpm.ttl = ttl
}

func (tbpm *TableBackedProgressManager) ensureTableExists() error {
	return tbpm.session.Query(
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s "+
				"(generation timestamp, table_name text, stream_id blob, last_timestamp timeuuid, current_generation timestamp, "+
				"PRIMARY KEY ((generation, table_name, stream_id)))",
			tbpm.progressTableName,
		),
	).Exec()
}

func (tbpm *TableBackedProgressManager) GetCurrentGeneration(ctx context.Context) (time.Time, error) {
	var gen time.Time
	err := tbpm.session.Query(
		fmt.Sprintf("SELECT current_generation FROM %s WHERE generation = ? AND table_name = ? AND stream_id = ?", tbpm.progressTableName),
		time.Time{}, "", []byte{},
	).Scan(&gen)

	if err != nil && err != gocql.ErrNotFound {
		return time.Time{}, err
	}
	return gen, nil
}

func (tbpm *TableBackedProgressManager) StartGeneration(ctx context.Context, gen time.Time) error {
	// Update the progress in the special partition
	return tbpm.session.Query(
		fmt.Sprintf(
			"INSERT INTO %s (generation, table_name, stream_id, current_generation) "+
				"VALUES (?, ?, ?, ?)",
			tbpm.progressTableName,
		),
		time.Time{}, "", []byte{}, gen,
	).Exec()
}

func (tbpm *TableBackedProgressManager) GetProgress(ctx context.Context, gen time.Time, tableName string, streamID StreamID) (Progress, error) {
	tbpm.concurrentQueryLimiter.Acquire(ctx, 1)
	defer tbpm.concurrentQueryLimiter.Release(1)

	var timestamp gocql.UUID
	err := tbpm.session.Query(
		fmt.Sprintf("SELECT last_timestamp FROM %s WHERE generation = ? AND table_name = ? AND stream_id = ?", tbpm.progressTableName),
		gen, tableName, streamID,
	).Scan(&timestamp)

	if err != nil && err != gocql.ErrNotFound {
		return Progress{}, err
	}
	return Progress{timestamp}, nil
}

func (tbpm *TableBackedProgressManager) SaveProgress(ctx context.Context, gen time.Time, tableName string, streamID StreamID, progress Progress) error {
	tbpm.concurrentQueryLimiter.Acquire(ctx, 1)
	defer tbpm.concurrentQueryLimiter.Release(1)

	return tbpm.session.Query(
		fmt.Sprintf("INSERT INTO %s (generation, table_name, stream_id, last_timestamp) VALUES (?, ?, ?, ?) USING TTL ?", tbpm.progressTableName),
		gen, tableName, streamID, progress.LastProcessedRecordTime, tbpm.ttl,
	).Exec()
}

var _ ProgressManager = (*TableBackedProgressManager)(nil)

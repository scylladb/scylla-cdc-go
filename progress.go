package scylla_cdc

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type ProgressReporter interface {
	// MarkProgress saves information about the last successfully processed
	// cdc log record. If the reader is restarted, it should resume reading
	// from this shard starting after the last marked position.
	MarkProgress(progress Progress) error
}

type ProgressManager interface {
	// GetCurrentGenerationTime returns the time of the generation that was
	// saved as being currently processed. If there was no such information
	// saved, it returns a zero value for Time.
	GetCurrentGeneration() (time.Time, error)

	// StartGeneration is called when a generation is discovered and the reader
	// should start processing the next generation.
	StartGeneration(gen time.Time) error

	// EndGeneration marks all streams in the generation as "done".
	EndGeneration(gen time.Time) error

	// GetProgress retrieves information about the progress of given shard,
	// in a given table.
	GetProgress(gen time.Time, table string, streamID StreamID) (Progress, error)

	// SaveProgress stores information about the last cdc log record which was
	// processed successfully. If the reader is restarted, it should resume
	// work for this shard from the last saved
	SaveProgress(gen time.Time, table string, streamID StreamID, progress Progress) error
}

type Progress struct {
	Time gocql.UUID
}

// NoProgressManager does not persist the progress at all. It can be used
// when saving progress is not necessary.
type NoProgressManager struct{}

func (*NoProgressManager) GetCurrentGeneration() (time.Time, error) {
	return time.Time{}, nil
}

func (*NoProgressManager) StartGeneration(gen time.Time) error {
	return nil
}

func (*NoProgressManager) EndGeneration(gen time.Time) error {
	return nil
}

func (*NoProgressManager) GetProgress(gen time.Time, table string, streamID StreamID) (Progress, error) {
	return Progress{}, nil
}

func (*NoProgressManager) SaveProgress(gen time.Time, table string, streamID StreamID, progress Progress) error {
	return nil
}

type TableBackedProgressManager struct {
	session           *gocql.Session
	progressTableName string

	// TTL to use when writing progress for a shard (a week by default).
	// TODO: maybe not? maybe we should clean up this data manually?
	// Progress data may be large if generations are very large
	ttl int32
}

func NewTableBackedProgressManager(session *gocql.Session, progressTableName string) (*TableBackedProgressManager, error) {
	tbpm := &TableBackedProgressManager{
		session:           session,
		progressTableName: progressTableName,

		ttl: 7 * 24 * 60 * 60, // 1 week
	}

	if err := tbpm.ensureTableExists(); err != nil {
		return nil, err
	}
	return tbpm, nil
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

func (tbpm *TableBackedProgressManager) GetCurrentGeneration() (time.Time, error) {
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

func (tbpm *TableBackedProgressManager) StartGeneration(gen time.Time) error {
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

func (tbpm *TableBackedProgressManager) EndGeneration(gen time.Time) error {
	// Do nothing. The partition which holds information about current generation
	// will be updated in StartGeneration.
	return nil
}

func (tbpm *TableBackedProgressManager) GetProgress(gen time.Time, tableName string, streamID StreamID) (Progress, error) {
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

func (tbpm *TableBackedProgressManager) SaveProgress(gen time.Time, tableName string, streamID StreamID, progress Progress) error {
	return tbpm.session.Query(
		fmt.Sprintf("INSERT INTO %s (generation, table_name, stream_id, last_timestamp) VALUES (?, ?, ?, ?) USING TTL ?", tbpm.progressTableName),
		gen, tableName, streamID, progress.Time, tbpm.ttl,
	).Exec()
}

var _ ProgressManager = (*TableBackedProgressManager)(nil)

type tableAndStreamProgressReporter struct {
	progressManager ProgressManager
	gen             time.Time
	tableName       string
	streamID        StreamID
}

func (taspr *tableAndStreamProgressReporter) MarkProgress(progress Progress) error {
	return taspr.progressManager.SaveProgress(taspr.gen, taspr.tableName, taspr.streamID, progress)
}

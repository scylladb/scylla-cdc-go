package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/piodul/scylla-cdc-go"
)

// TODO: Escape field names?
var showTimestamps = false
var debugQueries = false
var maxWaitBetweenRetries = 5 * time.Second

var reportPeriod = 1 * time.Minute

func main() {
	var (
		keyspace         string
		table            string
		source           string
		destination      string
		readConsistency  string
		writeConsistency string

		progressTable string
	)

	flag.StringVar(&keyspace, "keyspace", "", "keyspace name")
	flag.StringVar(&table, "table", "", "table name; you can specify multiple table by separating them with a comma")
	flag.StringVar(&source, "source", "", "address of a node in source cluster")
	flag.StringVar(&destination, "destination", "", "address of a node in destination cluster")
	flag.StringVar(&readConsistency, "read-consistency", "", "consistency level used to read from cdc log (one, quorum, all)")
	flag.StringVar(&writeConsistency, "write-consistency", "", "consistency level used to write to the destination cluster (one, quorum, all)")
	flag.StringVar(&progressTable, "progress-table", "", "fully-qualified name of the table in the destination cluster to use for saving progress; if omitted, the progress won't be saved")
	flag.String("mode", "", "mode (ignored)")
	flag.Parse()

	clRead := parseConsistency(readConsistency)
	clWrite := parseConsistency(writeConsistency)

	adv := scyllacdc.AdvancedReaderConfig{
		ConfidenceWindowSize:   30 * time.Second,
		ChangeAgeLimit:         10 * time.Minute,
		QueryTimeWindowSize:    60 * time.Second,
		PostEmptyQueryDelay:    30 * time.Second,
		PostNonEmptyQueryDelay: 10 * time.Second,
		PostFailedQueryDelay:   1 * time.Second,
	}

	fmt.Println("Parameters:")
	fmt.Printf("  Keyspace: %s\n", keyspace)
	fmt.Printf("  Table: %s\n", table)
	fmt.Printf("  Source cluster IP: %s\n", source)
	fmt.Printf("  Destination cluster IP: %s\n", destination)
	fmt.Printf("  Consistency for reads: %s\n", clRead)
	fmt.Printf("  Consistency for writes: %s\n", clWrite)
	fmt.Printf("  Table to use for saving progress: %s\n", progressTable)
	fmt.Println("Advanced reader parameters:")
	fmt.Printf("  Confidence window size: %s\n", adv.ConfidenceWindowSize)
	fmt.Printf("  Change age limit: %s\n", adv.ChangeAgeLimit)
	fmt.Printf("  Query window size: %s\n", adv.QueryTimeWindowSize)
	fmt.Printf("  Delay after poll with empty results: %s\n", adv.PostEmptyQueryDelay)
	fmt.Printf("  Delay after poll with non-empty results: %s\n", adv.PostNonEmptyQueryDelay)
	fmt.Printf("  Delay after failed poll: %s\n", adv.PostFailedQueryDelay)

	var fullyQualifiedTables []string

	for _, t := range strings.Split(table, ",") {
		fullyQualifiedTables = append(fullyQualifiedTables, keyspace+"."+t)
	}

	logger := log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	repl, err := newReplicator(
		context.Background(),
		source, destination,
		fullyQualifiedTables,
		&adv,
		clRead,
		clWrite,
		progressTable,
		logger,
	)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// React to Ctrl+C signal.
	//
	// 1st signal will cause the replicator to read changes up until
	// the moment the signal was received, and then it will stop the replicator.
	// This is the "most graceful" way of stopping the replicator.
	//
	// 2nd signal will cancel the context. This should stop all operations
	// done by the replicator ASAP and stop it.
	//
	// 3rd signal will exit the process immediately with error code 1.
	signalC := make(chan os.Signal)
	go func() {
		<-signalC
		now := time.Now()
		log.Printf("stopping at %v", now)
		repl.StopAt(now)

		<-signalC
		log.Printf("stopping now")
		cancel()

		<-signalC
		log.Printf("killing")
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	if err := repl.Run(ctx); err != nil {
		log.Fatalln(err)
	}

	log.Printf("quitting, rows read: %d", repl.GetReadRowsCount())
}

func parseConsistency(s string) gocql.Consistency {
	switch strings.ToLower(s) {
	case "one":
		return gocql.One
	case "quorum":
		return gocql.Quorum
	case "all":
		return gocql.All
	default:
		log.Printf("warning: got unsupported consistency level \"%s\", will use \"one\" instead", s)
		return gocql.One
	}
}

type replicator struct {
	reader *scyllacdc.Reader

	sourceSession      *gocql.Session
	destinationSession *gocql.Session

	rowsRead *int64
}

func newReplicator(
	ctx context.Context,
	source, destination string,
	tableNames []string,
	advancedParams *scyllacdc.AdvancedReaderConfig,
	readConsistency gocql.Consistency,
	writeConsistency gocql.Consistency,
	progressTable string,
	logger scyllacdc.Logger,
) (*replicator, error) {
	destinationCluster := gocql.NewCluster(destination)
	destinationCluster.Timeout = 10 * time.Second
	destinationSession, err := destinationCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	// Configure a session
	cluster := gocql.NewCluster(source)
	cluster.Timeout = 10 * time.Second
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	sourceSession, err := cluster.CreateSession()
	if err != nil {
		destinationSession.Close()
		return nil, err
	}

	rowsRead := new(int64)

	factory := &replicatorFactory{
		destinationSession: destinationSession,
		consistency:        writeConsistency,

		rowsRead: rowsRead,
		logger:   logger,
	}

	var progressManager scyllacdc.ProgressManager
	if progressTable != "" {
		progressManager, err = scyllacdc.NewTableBackedProgressManager(destinationSession, progressTable, "cdc-replicator")
		if err != nil {
			destinationSession.Close()
			return nil, err
		}
	}

	cfg := &scyllacdc.ReaderConfig{
		Session:               sourceSession,
		ChangeConsumerFactory: factory,
		ProgressManager:       progressManager,
		TableNames:            tableNames,
		Consistency:           readConsistency,
	}

	if advancedParams != nil {
		cfg.Advanced = *advancedParams
	}
	cfg.Consistency = readConsistency
	cfg.Logger = logger

	reader, err := scyllacdc.NewReader(ctx, cfg)
	if err != nil {
		sourceSession.Close()
		destinationSession.Close()
		return nil, err
	}

	repl := &replicator{
		reader: reader,

		sourceSession:      sourceSession,
		destinationSession: destinationSession,

		rowsRead: rowsRead,
	}

	return repl, nil
}

func (repl *replicator) Run(ctx context.Context) error {
	defer repl.destinationSession.Close()
	defer repl.sourceSession.Close()
	return repl.reader.Run(ctx)
}

func (repl *replicator) StopAt(at time.Time) {
	repl.reader.StopAt(at)
}

func (repl *replicator) Stop() {
	repl.reader.Stop()
}

func (repl *replicator) GetReadRowsCount() int64 {
	return atomic.LoadInt64(repl.rowsRead)
}

type replicatorFactory struct {
	destinationSession *gocql.Session
	consistency        gocql.Consistency

	rowsRead *int64
	logger   scyllacdc.Logger
}

func (rf *replicatorFactory) CreateChangeConsumer(
	ctx context.Context,
	input scyllacdc.CreateChangeConsumerInput,
) (scyllacdc.ChangeConsumer, error) {
	splitTableName := strings.SplitN(input.TableName, ".", 2)
	if len(splitTableName) < 2 {
		return nil, fmt.Errorf("table name is not fully qualified: %s", input.TableName)
	}

	kmeta, err := rf.destinationSession.KeyspaceMetadata(splitTableName[0])
	if err != nil {
		rf.destinationSession.Close()
		return nil, err
	}
	tmeta, ok := kmeta.Tables[splitTableName[1]]
	if !ok {
		rf.destinationSession.Close()
		return nil, fmt.Errorf("table %s does not exist", input.TableName)
	}

	return NewDeltaReplicator(ctx, rf.destinationSession, kmeta, tmeta, rf.consistency, rf.rowsRead, input.StreamID, input.ProgressReporter, rf.logger)
}

type DeltaReplicator struct {
	session     *gocql.Session
	tableName   string
	consistency gocql.Consistency

	pkColumns    []string
	ckColumns    []string
	otherColumns []string
	columnTypes  map[string]TypeInfo
	allColumns   []string

	insertStr               string
	rowDeleteQueryStr       string
	partitionDeleteQueryStr string

	localCount int64
	totalCount *int64

	streamID scyllacdc.StreamID
	reporter *scyllacdc.PeriodicProgressReporter
}

type updateQuerySet struct {
	add    string
	remove string
}

type udtInfo struct {
	setterQuery string
	fields      []string
}

func NewDeltaReplicator(
	ctx context.Context,
	session *gocql.Session,
	kmeta *gocql.KeyspaceMetadata,
	meta *gocql.TableMetadata,
	consistency gocql.Consistency,
	count *int64,
	streamID scyllacdc.StreamID,
	reporter *scyllacdc.ProgressReporter,
	logger scyllacdc.Logger,
) (*DeltaReplicator, error) {
	var (
		pkColumns    []string
		ckColumns    []string
		otherColumns []string
	)

	for _, name := range meta.OrderedColumns {
		colDesc := meta.Columns[name]
		switch colDesc.Kind {
		case gocql.ColumnPartitionKey:
			pkColumns = append(pkColumns, name)
		case gocql.ColumnClusteringKey:
			ckColumns = append(ckColumns, name)
		default:
			otherColumns = append(otherColumns, name)
		}
	}

	columnTypes := make(map[string]TypeInfo, len(meta.Columns))
	for colName, colMeta := range meta.Columns {
		info := parseType(colMeta.Type)
		columnTypes[colName] = info
	}

	dr := &DeltaReplicator{
		session:     session,
		tableName:   meta.Keyspace + "." + meta.Name,
		consistency: consistency,

		pkColumns:    pkColumns,
		ckColumns:    ckColumns,
		otherColumns: otherColumns,
		columnTypes:  columnTypes,
		allColumns:   append(append(append([]string{}, otherColumns...), pkColumns...), ckColumns...),

		totalCount: count,

		streamID: streamID,
		reporter: scyllacdc.NewPeriodicProgressReporter(logger, reportPeriod, reporter),
	}

	dr.precomputeQueries()

	dr.reporter.Start(ctx)
	return dr, nil
}

func (r *DeltaReplicator) precomputeQueries() {
	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)

	var bindMarkers []string
	for _, columnName := range keyColumns {
		bindMarkers = append(bindMarkers, makeBindMarkerForType(r.columnTypes[columnName]))
	}

	r.insertStr = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) USING TTL ?",
		r.tableName, strings.Join(keyColumns, ", "), strings.Join(bindMarkers, ", "),
	)

	r.rowDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		r.makeBindMarkerAssignments(keyColumns, " AND "),
	)

	r.partitionDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		r.makeBindMarkerAssignments(r.pkColumns, " AND "),
	)
}

func (r *DeltaReplicator) Consume(ctx context.Context, c scyllacdc.Change) error {
	timestamp := c.GetCassandraTimestamp()
	pos := 0

	if showTimestamps {
		log.Printf("[%s] Processing timestamp: %s (%s)\n", c.StreamID, c.Time, c.Time.Time())
	}

	for pos < len(c.Delta) {
		change := c.Delta[pos]
		var err error
		switch change.GetOperation() {
		case scyllacdc.Update:
			err = r.processUpdate(ctx, timestamp, change)
			pos++

		case scyllacdc.Insert:
			err = r.processInsert(ctx, timestamp, change)
			pos++

		case scyllacdc.RowDelete:
			err = r.processRowDelete(ctx, timestamp, change)
			pos++

		case scyllacdc.PartitionDelete:
			err = r.processPartitionDelete(ctx, timestamp, change)
			pos++

		case scyllacdc.RangeDeleteStartInclusive, scyllacdc.RangeDeleteStartExclusive:
			// Range delete start row should always be followed by a range delete end row.
			// They should always come in pairs.
			if pos+2 > len(c.Delta) {
				return errors.New("invalid change: range delete start row without corresponding end row")
			}
			start := change
			end := c.Delta[pos+1]
			if end.GetOperation() != scyllacdc.RangeDeleteEndInclusive && end.GetOperation() != scyllacdc.RangeDeleteEndExclusive {
				return errors.New("invalid change: range delete start row without corresponding end row")
			}
			err = r.processRangeDelete(ctx, timestamp, start, end)
			pos += 2

		case scyllacdc.RangeDeleteEndInclusive, scyllacdc.RangeDeleteEndExclusive:
			// This should not happen and indicates some kind of inconsistency.
			// Every RangeDeleteEnd... row should be preceded by a RangeDeleteStart... row.
			return errors.New("invalid change: range delete end row does not have a corresponding start row")

		default:
			return errors.New("unsupported operation: " + change.GetOperation().String())
		}

		if err != nil {
			return err
		}
	}

	r.reporter.Update(c.Time)
	r.localCount += int64(len(c.Delta))

	return nil
}

func (r *DeltaReplicator) End() error {
	log.Printf("Streams [%s]: processed %d changes in total", r.streamID, r.localCount)
	atomic.AddInt64(r.totalCount, r.localCount)
	_ = r.reporter.SaveAndStop(context.Background())
	return nil
}

func (r *DeltaReplicator) processUpdate(ctx context.Context, timestamp int64, c *scyllacdc.ChangeRow) error {
	return r.processInsertOrUpdate(ctx, timestamp, false, c)
}

func (r *DeltaReplicator) processInsert(ctx context.Context, timestamp int64, c *scyllacdc.ChangeRow) error {
	return r.processInsertOrUpdate(ctx, timestamp, true, c)
}

func (r *DeltaReplicator) processInsertOrUpdate(ctx context.Context, timestamp int64, isInsert bool, c *scyllacdc.ChangeRow) error {
	runQuery := func(q string, vals []interface{}) error {
		if debugQueries {
			fmt.Println(q)
			fmt.Println(vals...)
		}

		return tryWithExponentialBackoff(ctx, func() error {
			return r.session.
				Query(q, vals...).
				Consistency(r.consistency).
				Idempotent(true).
				WithTimestamp(timestamp).
				Exec()
		})
	}

	keyColumns := append(r.pkColumns, r.ckColumns...)

	if isInsert {
		// Insert row to make a row marker
		// The rest of the columns will be set by using UPDATE queries
		var vals []interface{}
		vals = appendKeyValuesToBind(vals, keyColumns, c)
		vals = append(vals, c.GetTTL())
		if err := runQuery(r.insertStr, vals); err != nil {
			return err
		}
	}

	// Precompute the WHERE x = ? AND y = ? ... part
	pkConditions := r.makeBindMarkerAssignments(keyColumns, " AND ")

	for _, colName := range r.otherColumns {
		typ := r.columnTypes[colName]
		isNonFrozenCollection := !typ.IsFrozen() && typ.Type().IsCollection()

		if !isNonFrozenCollection {
			atomicChange := c.GetAtomicChange(colName)
			if atomicChange.IsDeleted {
				// Delete the value from the column
				deleteStr := fmt.Sprintf(
					"DELETE %s FROM %s WHERE %s",
					colName, r.tableName, pkConditions,
				)

				var vals []interface{}
				vals = appendKeyValuesToBind(vals, keyColumns, c)
				if err := runQuery(deleteStr, vals); err != nil {
					return err
				}
			} else if !reflect.ValueOf(atomicChange.Value).IsNil() {
				// The column was overwritten
				updateStr := fmt.Sprintf(
					"UPDATE %s USING TTL ? SET %s = %s WHERE %s",
					r.tableName, colName, makeBindMarkerForType(typ), pkConditions,
				)

				var vals []interface{}
				vals = append(vals, c.GetTTL())
				vals = appendValueByType(vals, atomicChange.Value, typ)
				vals = appendKeyValuesToBind(vals, keyColumns, c)
				if err := runQuery(updateStr, vals); err != nil {
					return err
				}
			}
		} else if typ.Type() == TypeList {
			listChange := c.GetListChange(colName)
			if listChange.IsReset {
				// We can't just do UPDATE SET l = [...],
				// because we need to precisely control timestamps
				// of the list cells. This can be done only by
				// UPDATE tbl SET l[SCYLLA_TIMEUUID_LIST_INDEX(?)] = ?,
				// which is equivalent to an append of one cell.
				// Hence, the need for clear + append.
				//
				// We clear using a timestamp one-less-than the real
				// timestamp of the write. This is what Cassandra/Scylla
				// does internally, so it's OK to for us to do that.
				deleteStr := fmt.Sprintf(
					"DELETE %s FROM %s USING TIMESTAMP ? WHERE %s",
					colName, r.tableName, pkConditions,
				)

				var vals []interface{}
				vals = append(vals, timestamp-1)
				vals = appendKeyValuesToBind(vals, keyColumns, c)
				if err := runQuery(deleteStr, vals); err != nil {
					return err
				}
			}
			if !reflect.ValueOf(listChange.AppendedElements).IsNil() {
				// TODO: Explain
				setStr := fmt.Sprintf(
					"UPDATE %s USING TTL ? SET %s[SCYLLA_TIMEUUID_LIST_INDEX(?)] = %s WHERE %s",
					r.tableName, colName, makeBindMarkerForType(typ), pkConditions,
				)

				rAppendedElements := reflect.ValueOf(listChange.AppendedElements)
				r := rAppendedElements.MapRange()
				for r.Next() {
					k := r.Key().Interface()
					v := r.Value().Interface()

					var vals []interface{}
					vals = append(vals, c.GetTTL())
					vals = append(vals, k)
					vals = appendValueByType(vals, v, typ)
					vals = appendKeyValuesToBind(vals, keyColumns, c)
					if err := runQuery(setStr, vals); err != nil {
						return err
					}
				}
			}
			if !reflect.ValueOf(listChange.RemovedElements).IsNil() {
				// TODO: Explain
				clearStr := fmt.Sprintf(
					"UPDATE %s SET %s[SCYLLA_TIMEUUID_LIST_INDEX(?)] = null WHERE %s",
					r.tableName, colName, pkConditions,
				)

				rRemovedElements := reflect.ValueOf(listChange.RemovedElements)
				elsLen := rRemovedElements.Len()
				for i := 0; i < elsLen; i++ {
					k := rRemovedElements.Index(i).Interface()

					var vals []interface{}
					vals = append(vals, k)
					vals = appendKeyValuesToBind(vals, keyColumns, c)
					if err := runQuery(clearStr, vals); err != nil {
						return err
					}
				}
			}
		} else if typ.Type() == TypeSet || typ.Type() == TypeMap {
			// TODO: Better comment
			// Fortunately, both cases can be handled by the same code
			// by using reflection. We are forced to use reflection anyway.
			var (
				added, removed interface{}
				isReset        bool
			)

			if typ.Type() == TypeSet {
				setChange := c.GetSetChange(colName)
				added = setChange.AddedElements
				removed = setChange.RemovedElements
				isReset = setChange.IsReset
			} else {
				mapChange := c.GetSetChange(colName)
				added = mapChange.AddedElements
				removed = mapChange.RemovedElements
				isReset = mapChange.IsReset
			}

			if isReset {
				// Overwrite the existing value
				setStr := fmt.Sprintf(
					"UPDATE %s USING TTL ? SET %s = ? WHERE %s",
					r.tableName, colName, pkConditions,
				)

				var vals []interface{}
				vals = append(vals, c.GetTTL())
				vals = append(vals, added)
				vals = appendKeyValuesToBind(vals, keyColumns, c)
				if err := runQuery(setStr, vals); err != nil {
					return err
				}
			} else {
				if !reflect.ValueOf(added).IsNil() {
					// Add elements
					addStr := fmt.Sprintf(
						"UPDATE %s USING TTL ? SET %s = %s + ? WHERE %s",
						r.tableName, colName, colName, pkConditions,
					)

					var vals []interface{}
					vals = append(vals, c.GetTTL())
					vals = append(vals, added)
					vals = appendKeyValuesToBind(vals, keyColumns, c)
					if err := runQuery(addStr, vals); err != nil {
						return err
					}
				}
				if !reflect.ValueOf(removed).IsNil() {
					// Removed elements
					remStr := fmt.Sprintf(
						"UPDATE %s USING TTL ? SET %s = %s - ? WHERE %s",
						r.tableName, colName, colName, pkConditions,
					)

					var vals []interface{}
					vals = append(vals, c.GetTTL())
					vals = append(vals, removed)
					vals = appendKeyValuesToBind(vals, keyColumns, c)
					if err := runQuery(remStr, vals); err != nil {
						return err
					}
				}
			}
		} else if typ.Type() == TypeUDT {
			udtChange := c.GetUDTChange(colName)
			if udtChange.IsReset {
				// The column was overwritten
				updateStr := fmt.Sprintf(
					"UPDATE %s USING TTL ? SET %s = %s WHERE %s",
					r.tableName, colName, makeBindMarkerForType(typ), pkConditions,
				)

				var vals []interface{}
				vals = append(vals, c.GetTTL())
				vals = appendValueByType(vals, udtChange.AddedFields, typ)
				vals = appendKeyValuesToBind(vals, keyColumns, c)
				if err := runQuery(updateStr, vals); err != nil {
					return err
				}
			} else {
				// Overwrite those columns which are non-null in AddedFields,
				// and remove those which are listed in RemovedFields.
				var vals []interface{}
				vals = append(vals, c.GetTTL())
				fieldAssignments := make([]string, 0, len(udtChange.AddedFields)+len(udtChange.RemovedFields))

				// Overwrites
				for fieldName, fieldValue := range udtChange.AddedFields {
					if reflect.ValueOf(fieldValue).IsNil() {
						continue
					}

					// TODO: Properly create a bind marker, tuples nested in udts may cause problems
					fieldAssignments = append(fieldAssignments, fmt.Sprintf("%s.%s = ?", colName, fieldName))
					vals = append(vals, fieldValue)
				}

				// Clears
				for _, fieldName := range udtChange.RemovedFields {
					fieldAssignments = append(fieldAssignments, fmt.Sprintf("%s.%s = ?", colName, fieldName))
					vals = append(vals, nil)
				}

				vals = appendKeyValuesToBind(vals, keyColumns, c)

				updateUDTStr := fmt.Sprintf(
					"UPDATE %s USING TTL ? SET %s WHERE %s",
					r.tableName,
					strings.Join(fieldAssignments, ", "),
					pkConditions,
				)

				if err := runQuery(updateUDTStr, vals); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *DeltaReplicator) processRowDelete(ctx context.Context, timestamp int64, c *scyllacdc.ChangeRow) error {
	vals := make([]interface{}, 0, len(r.pkColumns)+len(r.ckColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)
	vals = appendKeyValuesToBind(vals, r.ckColumns, c)

	if debugQueries {
		fmt.Println(r.rowDeleteQueryStr)
		fmt.Println(vals...)
	}

	return tryWithExponentialBackoff(ctx, func() error {
		return r.session.
			Query(r.rowDeleteQueryStr, vals...).
			Consistency(r.consistency).
			Idempotent(true).
			WithTimestamp(timestamp).
			Exec()
	})
}

func (r *DeltaReplicator) processPartitionDelete(ctx context.Context, timestamp int64, c *scyllacdc.ChangeRow) error {
	vals := make([]interface{}, 0, len(r.pkColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)

	if debugQueries {
		fmt.Println(r.partitionDeleteQueryStr)
		fmt.Println(vals...)
	}

	return tryWithExponentialBackoff(ctx, func() error {
		return r.session.
			Query(r.partitionDeleteQueryStr, vals...).
			Consistency(r.consistency).
			Idempotent(true).
			WithTimestamp(timestamp).
			Exec()
	})
}

func (r *DeltaReplicator) processRangeDelete(ctx context.Context, timestamp int64, start, end *scyllacdc.ChangeRow) error {
	vals := make([]interface{}, 0)
	vals = appendKeyValuesToBind(vals, r.pkColumns, start)

	conditions := r.makeBindMarkerAssignmentList(r.pkColumns)

	addConditions := func(c *scyllacdc.ChangeRow, cmpOp string) {
		ckNames := make([]string, 0)
		markers := make([]string, 0)
		for _, ckCol := range r.ckColumns {

			// Clustering key values are always atomic
			ckVal, _ := c.GetValue(ckCol)
			if reflect.ValueOf(ckVal).IsNil() {
				break
			}
			ckNames = append(ckNames, ckCol)
			vals = append(vals, ckVal)
			markers = append(markers, makeBindMarkerForType(r.columnTypes[ckCol]))
		}

		if len(ckNames) > 0 {
			conditions = append(conditions, fmt.Sprintf(
				"(%s) %s (%s)",
				strings.Join(ckNames, ", "),
				cmpOp,
				strings.Join(markers, ", "),
			))
		}
	}

	if start.GetOperation() == scyllacdc.RangeDeleteStartInclusive {
		addConditions(start, ">=")
	} else {
		addConditions(start, ">")
	}

	if end.GetOperation() == scyllacdc.RangeDeleteEndInclusive {
		addConditions(end, "<=")
	} else {
		addConditions(end, "<")
	}

	deleteStr := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		strings.Join(conditions, " AND "),
	)

	if debugQueries {
		fmt.Println(deleteStr)
		fmt.Println(vals...)
	}

	return tryWithExponentialBackoff(ctx, func() error {
		return r.session.
			Query(deleteStr, vals...).
			Consistency(r.consistency).
			Idempotent(true).
			WithTimestamp(timestamp).
			Exec()
	})
}

func (r *DeltaReplicator) makeBindMarkerAssignmentList(columnNames []string) []string {
	assignments := make([]string, 0, len(columnNames))
	for _, name := range columnNames {
		assignments = append(assignments, name+" = "+makeBindMarkerForType(r.columnTypes[name]))
	}
	return assignments
}

func (r *DeltaReplicator) makeBindMarkerAssignments(columnNames []string, sep string) string {
	assignments := r.makeBindMarkerAssignmentList(columnNames)
	return strings.Join(assignments, sep)
}

func makeBindMarkerForType(typ TypeInfo) string {
	if typ.Type() != TypeTuple {
		return "?"
	}
	tupleTyp := typ.Unfrozen().(*TupleType)
	vals := make([]string, 0, len(tupleTyp.Elements))
	for _, el := range tupleTyp.Elements {
		vals = append(vals, makeBindMarkerForType(el))
	}
	return "(" + strings.Join(vals, ", ") + ")"
}

func appendValueByType(vals []interface{}, v interface{}, typ TypeInfo) []interface{} {
	if typ.Type() == TypeTuple {
		tupTyp := typ.Unfrozen().(*TupleType)

		var vTup []interface{}
		switch v := v.(type) {
		case []interface{}:
			if v != nil {
				vTup = v
			} else {
				vTup = make([]interface{}, len(tupTyp.Elements))
			}
		case *[]interface{}:
			if v != nil {
				vTup = *v
			} else {
				vTup = make([]interface{}, len(tupTyp.Elements))
			}
		case nil:
			vTup = make([]interface{}, len(tupTyp.Elements))
		default:
			panic(fmt.Sprintf("unhandled tuple type: %t", v))
		}

		for i, vEl := range vTup {
			elTyp := tupTyp.Elements[i]
			vals = appendValueByType(vals, vEl, elTyp)
		}
	} else {
		vals = append(vals, v)
	}
	return vals
}

func appendKeyValuesToBind(
	vals []interface{},
	names []string,
	c *scyllacdc.ChangeRow,
) []interface{} {
	// No need to handle non-frozen lists here, because they can't appear
	// in either partition or clustering key
	// TODO: Support tuples here, too
	for _, name := range names {
		v, _ := c.GetValue(name)
		if reflect.ValueOf(v).IsNil() {
			v = gocql.UnsetValue
		}
		vals = append(vals, v)
	}
	return vals
}

func tryWithExponentialBackoff(ctx context.Context, f func() error) error {
	dur := 50 * time.Millisecond
	var err error
	// TODO: Make it stop when the replicator is shut down
	i := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = f()
		if err == nil {
			return nil
		}

		log.Printf("ERROR (try #%d): %s", i+1, err)
		i++

		<-time.After(dur)

		// Increase backoff duration randomly - between 2 to 3 times
		factor := 2.0 + rand.Float64()*3.0
		dur = time.Duration(float64(dur) * factor)
		if dur > maxWaitBetweenRetries {
			dur = maxWaitBetweenRetries
		}
	}
}

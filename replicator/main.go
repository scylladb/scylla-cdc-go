package main

import (
	"context"
	"encoding/hex"
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
	scylla_cdc "github.com/piodul/scylla-cdc-go"
)

// TODO: Escape field names?

var showTimestamps = false
var debugQueries = false
var maxWaitBetweenRetries = 5 * time.Second

func main() {
	var (
		keyspace         string
		table            string
		source           string
		destination      string
		readConsistency  string
		writeConsistency string
	)

	flag.StringVar(&keyspace, "keyspace", "", "keyspace name")
	flag.StringVar(&table, "table", "", "table name; you can specify multiple table by separating them with a comma")
	flag.StringVar(&source, "source", "", "address of a node in source cluster")
	flag.StringVar(&destination, "destination", "", "address of a node in destination cluster")
	flag.StringVar(&readConsistency, "read-consistency", "", "consistency level used to read from cdc log (one, quorum, all)")
	flag.StringVar(&writeConsistency, "write-consistency", "", "consistency level used to write to the destination cluster (one, quorum, all)")
	flag.String("mode", "", "mode (ignored)")
	flag.Parse()

	clRead := parseConsistency(readConsistency)
	clWrite := parseConsistency(writeConsistency)

	adv := scylla_cdc.AdvancedReaderConfig{
		ConfidenceWindowSize:   30 * time.Second,
		ChangeAgeLimit:         24 * time.Hour,
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
	fmt.Println("Advanced reader parameters:")
	fmt.Printf("  Confidence window size: %s\n", adv.ConfidenceWindowSize)
	fmt.Printf("  Change age limit: %s\n", adv.ChangeAgeLimit)
	fmt.Printf("  Query window size: %s\n", adv.QueryTimeWindowSize)
	fmt.Printf("  Delay after poll with empty results: %s\n", adv.PostEmptyQueryDelay)
	fmt.Printf("  Delay after poll with non-empty results: %s\n", adv.PostNonEmptyQueryDelay)
	fmt.Printf("  Delay after failed poll: %s\n", adv.PostEmptyQueryDelay)

	var fullyQualifiedTables []string

	for _, t := range strings.Split(table, ",") {
		fullyQualifiedTables = append(fullyQualifiedTables, keyspace+"."+t)
	}

	logger := log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	repl, err := newReplicator(
		source, destination,
		fullyQualifiedTables,
		&adv,
		clRead,
		clWrite,
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
	reader *scylla_cdc.Reader

	sourceSession      *gocql.Session
	destinationSession *gocql.Session

	rowsRead *int64
}

func newReplicator(
	source, destination string,
	tableNames []string,
	advancedParams *scylla_cdc.AdvancedReaderConfig,
	readConsistency gocql.Consistency,
	writeConsistency gocql.Consistency,
	logger scylla_cdc.Logger,
) (*replicator, error) {
	destinationCluster := gocql.NewCluster(destination)
	destinationSession, err := destinationCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	// Configure a session
	cluster := gocql.NewCluster(source)
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
	}

	// Configuration for the CDC reader
	// TODO: Allow specifying a progress manager
	cfg := scylla_cdc.NewReaderConfig(
		sourceSession,
		factory,
		&scylla_cdc.NoProgressManager{},
		tableNames...,
	)
	if advancedParams != nil {
		cfg.Advanced = *advancedParams
	}
	cfg.Consistency = readConsistency
	cfg.Logger = logger

	reader, err := scylla_cdc.NewReader(cfg)
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
}

func (rf *replicatorFactory) CreateChangeConsumer(input scylla_cdc.CreateChangeConsumerInput) (scylla_cdc.ChangeConsumer, error) {
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

	return NewDeltaReplicator(rf.destinationSession, kmeta, tmeta, rf.consistency, rf.rowsRead, input.StreamIDs)
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

	streamIDs []scylla_cdc.StreamID
}

type updateQuerySet struct {
	add    string
	remove string
}

type udtInfo struct {
	setterQuery string
	fields      []string
}

func NewDeltaReplicator(session *gocql.Session, kmeta *gocql.KeyspaceMetadata, meta *gocql.TableMetadata, consistency gocql.Consistency, count *int64, streamIDs []scylla_cdc.StreamID) (*DeltaReplicator, error) {
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

		streamIDs: streamIDs,
	}

	dr.precomputeQueries()

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

func (r *DeltaReplicator) Consume(c scylla_cdc.Change) error {
	timestamp := c.GetCassandraTimestamp()
	pos := 0

	if showTimestamps {
		log.Printf("Processing timestamp: %v\n", c.Time)
	}

	for pos < len(c.Delta) {
		change := c.Delta[pos]
		var err error
		switch change.GetOperation() {
		case scylla_cdc.Update:
			err = r.processUpdate(timestamp, change)
			pos++

		case scylla_cdc.Insert:
			err = r.processInsert(timestamp, change)
			pos++

		case scylla_cdc.RowDelete:
			err = r.processRowDelete(timestamp, change)
			pos++

		case scylla_cdc.PartitionDelete:
			err = r.processPartitionDelete(timestamp, change)
			pos++

		case scylla_cdc.RangeDeleteStartInclusive, scylla_cdc.RangeDeleteStartExclusive:
			// Range delete start row should always be followed by a range delete end row.
			// They should always come in pairs.
			if pos+2 > len(c.Delta) {
				return errors.New("invalid change: range delete start row without corresponding end row")
			}
			start := change
			end := c.Delta[pos+1]
			if end.GetOperation() != scylla_cdc.RangeDeleteEndInclusive && end.GetOperation() != scylla_cdc.RangeDeleteEndExclusive {
				return errors.New("invalid change: range delete start row without corresponding end row")
			}
			err = r.processRangeDelete(timestamp, start, end)
			pos += 2

		case scylla_cdc.RangeDeleteEndInclusive, scylla_cdc.RangeDeleteEndExclusive:
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

	r.localCount += int64(len(c.Delta))

	return nil
}

func (r *DeltaReplicator) End() {
	var hexStreams []string
	for _, id := range r.streamIDs {
		hexStreams = append(hexStreams, hex.EncodeToString(id))
	}
	log.Printf("Streams [%s]: processed %d changes in total", strings.Join(hexStreams, ", "), r.localCount)
	atomic.AddInt64(r.totalCount, r.localCount)
}

func (r *DeltaReplicator) processUpdate(timestamp int64, c *scylla_cdc.ChangeRow) error {
	return r.processInsertOrUpdate(timestamp, false, c)
}

func (r *DeltaReplicator) processInsert(timestamp int64, c *scylla_cdc.ChangeRow) error {
	return r.processInsertOrUpdate(timestamp, true, c)
}

func (r *DeltaReplicator) processInsertOrUpdate(timestamp int64, isInsert bool, c *scylla_cdc.ChangeRow) error {
	runQuery := func(q string, vals []interface{}) error {
		if debugQueries {
			fmt.Println(q)
			fmt.Println(vals...)
		}

		return tryWithExponentialBackoff(func() error {
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
			scalarChange := c.GetScalarChange(colName)
			if scalarChange.IsDeleted {
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
			} else if scalarChange.Value != nil {
				// The column was overwritten
				updateStr := fmt.Sprintf(
					"UPDATE %s USING TTL ? SET %s = %s WHERE %s",
					r.tableName, colName, makeBindMarkerForType(typ), pkConditions,
				)

				var vals []interface{}
				vals = append(vals, c.GetTTL())
				vals = appendValueByType(vals, scalarChange.Value, typ)
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
			if listChange.AppendedElements != nil {
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
			if listChange.RemovedElements != nil {
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
				if added != nil {
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
				if removed != nil {
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

func (r *DeltaReplicator) processRowDelete(timestamp int64, c *scylla_cdc.ChangeRow) error {
	vals := make([]interface{}, 0, len(r.pkColumns)+len(r.ckColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)
	vals = appendKeyValuesToBind(vals, r.ckColumns, c)

	if debugQueries {
		fmt.Println(r.rowDeleteQueryStr)
		fmt.Println(vals...)
	}

	return tryWithExponentialBackoff(func() error {
		return r.session.
			Query(r.rowDeleteQueryStr, vals...).
			Consistency(r.consistency).
			Idempotent(true).
			WithTimestamp(timestamp).
			Exec()
	})
}

func (r *DeltaReplicator) processPartitionDelete(timestamp int64, c *scylla_cdc.ChangeRow) error {
	vals := make([]interface{}, 0, len(r.pkColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)

	if debugQueries {
		fmt.Println(r.partitionDeleteQueryStr)
		fmt.Println(vals...)
	}

	return tryWithExponentialBackoff(func() error {
		return r.session.
			Query(r.partitionDeleteQueryStr, vals...).
			Consistency(r.consistency).
			Idempotent(true).
			WithTimestamp(timestamp).
			Exec()
	})
}

func (r *DeltaReplicator) processRangeDelete(timestamp int64, start, end *scylla_cdc.ChangeRow) error {
	vals := make([]interface{}, 0)
	vals = appendKeyValuesToBind(vals, r.pkColumns, start)

	conditions := r.makeBindMarkerAssignmentList(r.pkColumns)

	addConditions := func(c *scylla_cdc.ChangeRow, cmpOp string) {
		ckNames := make([]string, 0)
		markers := make([]string, 0)
		for _, ckCol := range r.ckColumns {

			// Clustering key values are always atomic
			ckVal, ok := c.GetValue(ckCol)
			if !ok {
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

	if start.GetOperation() == scylla_cdc.RangeDeleteStartInclusive {
		addConditions(start, ">=")
	} else {
		addConditions(start, ">")
	}

	if end.GetOperation() == scylla_cdc.RangeDeleteEndInclusive {
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

	return tryWithExponentialBackoff(func() error {
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
			vTup = v
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
	c *scylla_cdc.ChangeRow,
) []interface{} {
	// No need to handle non-frozen lists here, because they can't appear
	// in either partition or clustering key
	// TODO: Support tuples here, too
	for _, name := range names {
		v, ok := c.GetValue(name)
		if !ok {
			v = gocql.UnsetValue
		}
		vals = append(vals, v)
	}
	return vals
}

func tryWithExponentialBackoff(f func() error) error {
	dur := 50 * time.Millisecond
	var err error
	// TODO: Make it stop when the replicator is shut down
	i := 0
	for {
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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
// TODO: Tuple support

var showTimestamps = true
var debugQueries = false
var retryCount = 20
var maxWaitBetweenRetries = 5 * time.Second

func main() {
	var (
		keyspace    string
		table       string
		source      string
		destination string
		consistency string
	)

	flag.StringVar(&keyspace, "keyspace", "", "keyspace name")
	flag.StringVar(&table, "table", "", "table name")
	flag.StringVar(&source, "source", "", "address of a node in source cluster")
	flag.StringVar(&destination, "destination", "", "address of a node in destination cluster")
	flag.StringVar(&consistency, "consistency", "", "consistency level (one, quorum, all)")
	flag.String("mode", "", "mode (ignored)")
	flag.Parse()

	cl := gocql.One
	switch strings.ToLower(consistency) {
	case "one":
		cl = gocql.One
	case "quorum":
		cl = gocql.Quorum
	case "all":
		cl = gocql.All
	}

	adv := scylla_cdc.AdvancedReaderConfig{
		ConfidenceWindowSize:   30 * time.Second,
		ChangeAgeLimit:         24 * time.Hour,
		QueryTimeWindowSize:    24 * time.Hour,
		PostEmptyQueryDelay:    30 * time.Second,
		PostNonEmptyQueryDelay: 10 * time.Second,
		PostFailedQueryDelay:   1 * time.Second,
	}

	reader, rowsRead, err := MakeReplicator(
		source, destination,
		[]string{keyspace + "." + table},
		&adv,
		cl,
	)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// React to Ctrl+C signal, and stop gracefully after the first signal
	// Second signal cancels the context, so that the replicator
	// should stop immediately, but still gracefully
	// The third signal kills the process
	signalC := make(chan os.Signal)
	go func() {
		<-signalC
		now := time.Now()
		log.Printf("stopping at %v", now)
		reader.StopAt(now)

		<-signalC
		log.Printf("stopping now")
		cancel()

		<-signalC
		log.Printf("killing")
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	if err := reader.Run(ctx); err != nil {
		log.Println(err)
		os.Exit(1)
	}

	log.Printf("quitting, rows read: %d", *rowsRead)
}

func MakeReplicator(
	source, destination string,
	tableNames []string,
	advancedParams *scylla_cdc.AdvancedReaderConfig,
	consistency gocql.Consistency,
) (*scylla_cdc.Reader, *int64, error) {
	// Configure a session for the destination cluster
	destinationCluster := gocql.NewCluster(destination)
	destinationSession, err := destinationCluster.CreateSession()
	if err != nil {
		return nil, nil, err
	}

	tracker := scylla_cdc.NewClusterStateTracker(gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()))

	// Configure a session
	cluster := gocql.NewCluster(source)
	cluster.PoolConfig.HostSelectionPolicy = tracker
	session, err := cluster.CreateSession()
	if err != nil {
		destinationSession.Close()
		return nil, nil, err
	}

	progressManager, err := scylla_cdc.NewTableBackedProgressManager(session, "ks.cdc_go_replicator_progress")
	if err != nil {
		return nil, nil, err
	}

	rowsRead := new(int64)

	factory := &replicatorFactory{
		destinationSession: destinationSession,
		consistency:        consistency,

		rowsRead: rowsRead,
	}

	// Configuration for the CDC reader
	cfg := scylla_cdc.NewReaderConfig(
		session,
		factory,
		progressManager,
		tableNames...,
	)
	if advancedParams != nil {
		cfg.Advanced = *advancedParams
	}
	cfg.Consistency = consistency
	cfg.ClusterStateTracker = tracker
	cfg.Logger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	reader, err := scylla_cdc.NewReader(cfg)
	if err != nil {
		session.Close()
		destinationSession.Close()
		return nil, nil, err
	}

	// TODO: source and destination sessions are leaking
	return reader, rowsRead, nil
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

	return NewDeltaReplicator(rf.destinationSession, kmeta, tmeta, rf.consistency, rf.rowsRead)
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

	rowDeleteQueryStr       string
	partitionDeleteQueryStr string
	rangeDeleteQueryStrs    []string

	localCount int64
	totalCount *int64
}

type updateQuerySet struct {
	add    string
	remove string
}

type udtInfo struct {
	setterQuery string
	fields      []string
}

func NewDeltaReplicator(session *gocql.Session, kmeta *gocql.KeyspaceMetadata, meta *gocql.TableMetadata, consistency gocql.Consistency, count *int64) (*DeltaReplicator, error) {
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
	}

	dr.computeRowDeleteQuery()
	dr.computePartitionDeleteQuery()

	return dr, nil
}

func (r *DeltaReplicator) computeRowDeleteQuery() {
	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)

	r.rowDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		r.makeBindMarkerAssignments(keyColumns, " AND "),
	)
}

func (r *DeltaReplicator) computePartitionDeleteQuery() {
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
			// TODO: Check that we aren't at the end?
			start := change
			end := c.Delta[pos+1]
			err = r.processRangeDelete(timestamp, start, end)
			pos += 2

		default:
			panic("unsupported operation: " + change.GetOperation().String())
		}

		if err != nil {
			return err
		}
	}

	r.localCount += int64(len(c.Delta))

	return nil
}

func (r *DeltaReplicator) End() {
	// TODO: Take a snapshot here
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
		var bindMarkers []string
		for _, columnName := range keyColumns {
			bindMarkers = append(bindMarkers, makeBindMarkerForType(r.columnTypes[columnName]))
		}

		insertStr := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) USING TTL ?",
			r.tableName, strings.Join(keyColumns, ", "), strings.Join(bindMarkers, ", "),
		)

		var vals []interface{}
		vals = appendKeyValuesToBind(vals, keyColumns, c)
		vals = append(vals, c.GetTTL())
		if err := runQuery(insertStr, vals); err != nil {
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
				clearTimestamp := timestamp
				if listChange.AppendedElements != nil {
					clearTimestamp--
				}
				vals = append(vals, clearTimestamp)
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
				// In order to do this, we need to know the schema
				// of the UDT.

				// TODO: Optimize, this makes processing of the row quadratic
				colInfos := c.Columns()
				var udtInfo gocql.UDTTypeInfo
				for _, colInfo := range colInfos {
					if colInfo.Name == colName {
						udtInfo = colInfo.TypeInfo.(gocql.UDTTypeInfo)
						break
					}
				}

				elementValues := make([]interface{}, len(udtInfo.Elements))

				// Determine which elements to set, which to remove and which to ignore
				for i := range elementValues {
					elementValues[i] = gocql.UnsetValue
				}
				for i, el := range udtInfo.Elements {
					v := udtChange.AddedFields[el.Name]
					// TODO: Do we want to use pointers in maps?
					if v != nil && !reflect.ValueOf(v).IsNil() {
						elementValues[i] = v
					}
				}
				for _, idx := range udtChange.RemovedFields {
					elementValues[idx] = nil
				}

				// Send an individual query for each field that is being updated
				for i, el := range udtInfo.Elements {
					v := elementValues[i]
					if v == gocql.UnsetValue {
						continue
					}

					// fmt.Printf("    %#v\n", v)

					bindValue := "null"
					if v != nil {
						// TODO: This should be "typ" for the UDT element
						bindValue = makeBindMarkerForType(typ)
					}

					updateFieldStr := fmt.Sprintf(
						"UPDATE %s USING TTL ? SET %s.%s = %s WHERE %s",
						r.tableName, colName, el.Name, bindValue, pkConditions,
					)

					var vals []interface{}
					vals = append(vals, c.GetTTL())
					if v != nil {
						vals = appendValueByType(vals, v, typ)
					}
					vals = appendKeyValuesToBind(vals, keyColumns, c)
					if err := runQuery(updateFieldStr, vals); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (r *DeltaReplicator) processRowDelete(timestamp int64, c *scylla_cdc.ChangeRow) error {
	// TODO: Cache vals?
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
	// TODO: Cache vals?
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
	// TODO: Cache vals?
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
	for i := 0; i < retryCount; i++ {
		err = f()
		if err == nil {
			return nil
		}

		log.Printf("ERROR (%d/%d): %s", i+1, retryCount, err)

		<-time.After(dur)
		dur *= 2
		if dur > maxWaitBetweenRetries {
			dur = maxWaitBetweenRetries
		}
	}

	return err
}

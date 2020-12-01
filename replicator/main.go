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

	"github.com/gocql/gocql"
	scylla_cdc "github.com/piodul/scylla-cdc-go"
)

// TODO: Escape field names?
// TODO: Tuple support

var debugQueries = true

func main() {
	var (
		keyspace    string
		table       string
		source      string
		destination string
	)

	flag.StringVar(&keyspace, "keyspace", "", "keyspace name")
	flag.StringVar(&table, "table", "", "table name")
	flag.StringVar(&source, "source", "", "address of a node in source cluster")
	flag.StringVar(&destination, "destination", "", "address of a node in destination cluster")
	flag.Parse()

	finishFunc, err := RunReplicator(
		context.Background(),
		source, destination,
		[]string{keyspace + "." + table},
		nil,
	)
	if err != nil {
		log.Fatalln(err)
	}

	// React to Ctrl+C signal, and stop gracefully after the first signal
	// Second signal exits the process
	// TODO: The stopping process here could be a little nicer
	signalC := make(chan os.Signal)
	go func() {
		<-signalC
		go func() {
			err := finishFunc()
			if err != nil {
				log.Fatalln(err)
			}
			os.Exit(0)
		}()

		<-signalC
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	select {}
}

func RunReplicator(
	ctx context.Context,
	source, destination string,
	tableNames []string,
	advancedParams *scylla_cdc.AdvancedReaderConfig,
) (func() error, error) {
	// Configure a session for the destination cluster
	destinationCluster := gocql.NewCluster(destination)
	destinationSession, err := destinationCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	tableReplicators := make(map[string]scylla_cdc.ChangeConsumer)
	logTableNames := make([]string, 0)

	for _, tableName := range tableNames {
		splitTableName := strings.SplitN(tableName, ".", 2)
		if len(splitTableName) < 2 {
			return nil, fmt.Errorf("table name is not fully qualified: %s", tableName)
		}

		kmeta, err := destinationSession.KeyspaceMetadata(splitTableName[0])
		if err != nil {
			destinationSession.Close()
			return nil, err
		}
		tmeta, ok := kmeta.Tables[splitTableName[1]]
		if !ok {
			destinationSession.Close()
			return nil, fmt.Errorf("table %s does not exist", tableName)
		}
		tableReplicators[tableName+"_scylla_cdc_log"] = NewDeltaReplicator(destinationSession, tmeta)
		logTableNames = append(logTableNames, tableName+"_scylla_cdc_log")
	}

	replicator := &TableRoutingConsumer{tableReplicators}

	tracker := scylla_cdc.NewClusterStateTracker(gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()))

	// Configure a session
	cluster := gocql.NewCluster(source)
	cluster.PoolConfig.HostSelectionPolicy = tracker
	session, err := cluster.CreateSession()
	if err != nil {
		destinationSession.Close()
		return nil, err
	}

	// Configuration for the CDC reader
	cfg := scylla_cdc.NewReaderConfig(
		session,
		replicator,
		logTableNames...,
	)
	if advancedParams != nil {
		cfg.Advanced = *advancedParams
	}
	cfg.ClusterStateTracker = tracker
	cfg.Logger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	reader, err := scylla_cdc.NewReader(cfg)
	if err != nil {
		session.Close()
		destinationSession.Close()
		return nil, err
	}

	errC := make(chan error)
	go func() {
		errC <- reader.Run(ctx)
	}()

	return func() error {
		reader.Stop()
		err := <-errC
		session.Close()
		destinationSession.Close()
		return err
	}, nil
}

// TODO: We could actually put this one in the lib
type TableRoutingConsumer struct {
	tableConsumers map[string]scylla_cdc.ChangeConsumer
}

func (trc *TableRoutingConsumer) Consume(tableName string, change scylla_cdc.Change) {
	consumer, present := trc.tableConsumers[tableName]
	if present {
		consumer.Consume(tableName, change)
	}
}

type DeltaReplicator struct {
	session     *gocql.Session
	tableName   string
	consistency gocql.Consistency

	pkColumns        []string
	ckColumns        []string
	otherColumns     []string
	otherColumnTypes []TypeInfo
	allColumns       []string

	insertQueryStr          string
	updateQueryStr          string
	perColumnUpdateQueries  []updateQuerySet
	rowDeleteQueryStr       string
	partitionDeleteQueryStr string
	rangeDeleteQueryStrs    []string
}

type updateQuerySet struct {
	add    string
	remove string
}

func NewDeltaReplicator(session *gocql.Session, meta *gocql.TableMetadata) *DeltaReplicator {
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

	otherColumnTypes := make([]TypeInfo, 0, len(otherColumns))
	for _, colName := range otherColumns {
		info := parseType(meta.Columns[colName].Type)
		otherColumnTypes = append(otherColumnTypes, info)
	}

	dr := &DeltaReplicator{
		session:     session,
		tableName:   meta.Keyspace + "." + meta.Name,
		consistency: gocql.Quorum,

		pkColumns:        pkColumns,
		ckColumns:        ckColumns,
		otherColumns:     otherColumns,
		otherColumnTypes: otherColumnTypes,
		allColumns:       append(append(append([]string{}, otherColumns...), pkColumns...), ckColumns...),
	}

	dr.computeInsertQuery()
	dr.computeUpdateQueries()
	dr.computeRowDeleteQuery()
	dr.computePartitionDeleteQuery()
	dr.computeRangeDeleteQueries()

	return dr
}

func (r *DeltaReplicator) computeUpdateQueries() {
	// Compute the regular update query
	assignments := makeBindMarkerAssignments(r.otherColumns, ", ")

	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)
	conditions := makeBindMarkerAssignments(keyColumns, " AND ")

	r.updateQueryStr = fmt.Sprintf(
		"UPDATE %s USING TTL ? SET %s WHERE %s",
		r.tableName,
		assignments,
		conditions,
	)

	// Compute per-column update queries (only for non-frozen collections)
	queries := make([]updateQuerySet, len(r.otherColumns))
	for i, colName := range r.otherColumns {
		typ := r.otherColumnTypes[i]
		if typ.IsFrozen() {
			continue
		}

		switch typ.Type() {
		case TypeList:
			queries[i].add = fmt.Sprintf(
				"UPDATE %s USING TTL ? SET %s[SCYLLA_TIMEUUID_LIST_INDEX(?)] = ? WHERE %s",
				r.tableName,
				colName,
				conditions,
			)
			queries[i].remove = fmt.Sprintf(
				"UPDATE %s USING TTL ? AND TIMESTAMP ? SET %s = null WHERE %s",
				r.tableName,
				colName,
				conditions,
			)
		case TypeMap, TypeSet:
			queries[i].add = fmt.Sprintf(
				"UPDATE %s USING TTL ? SET %s = %s + ? WHERE %s",
				r.tableName,
				colName, colName,
				conditions,
			)
			queries[i].remove = fmt.Sprintf(
				"UPDATE %s USING TTL ? SET %s = %s - ? WHERE %s",
				r.tableName,
				colName, colName,
				conditions,
			)
		}
	}

	r.perColumnUpdateQueries = queries
}

func (r *DeltaReplicator) computeInsertQuery() {
	namesTuple := "(" + strings.Join(r.allColumns, ", ") + ")"
	valueMarkersTuple := "(?" + strings.Repeat(", ?", len(r.allColumns)-1) + ")"

	r.insertQueryStr = fmt.Sprintf(
		"INSERT INTO %s %s VALUES %s USING TTL ?",
		r.tableName,
		namesTuple,
		valueMarkersTuple,
	)
}

func (r *DeltaReplicator) computeRowDeleteQuery() {
	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)

	r.rowDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		makeBindMarkerAssignments(keyColumns, " AND "),
	)
}

func (r *DeltaReplicator) computePartitionDeleteQuery() {
	r.partitionDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		makeBindMarkerAssignments(r.pkColumns, " AND "),
	)
}

func (r *DeltaReplicator) computeRangeDeleteQueries() {
	r.rangeDeleteQueryStrs = make([]string, 0, 8*len(r.ckColumns))

	prefix := fmt.Sprintf("DELETE FROM %s WHERE ", r.tableName)
	eqConds := makeBindMarkerAssignmentList(r.pkColumns)

	for _, ckCol := range r.ckColumns {
		for typ := 0; typ < 8; typ++ {
			startOp := [3]string{">=", ">", ""}[typ%3]
			endOp := [3]string{"<=", "<", ""}[typ/3]
			condsWithBounds := eqConds
			if startOp != "" {
				condsWithBounds = append(
					condsWithBounds,
					fmt.Sprintf("%s %s ?", ckCol, startOp),
				)
			}
			if endOp != "" {
				condsWithBounds = append(
					condsWithBounds,
					fmt.Sprintf("%s %s ?", ckCol, endOp),
				)
			}
			queryStr := prefix + strings.Join(condsWithBounds, " AND ")
			r.rangeDeleteQueryStrs = append(r.rangeDeleteQueryStrs, queryStr)
		}

		eqConds = append(eqConds, ckCol+" = ?")
	}
}

func (r *DeltaReplicator) Consume(tableName string, c scylla_cdc.Change) {
	timestamp := c.GetCassandraTimestamp()
	pos := 0

	for pos < len(c.Delta) {
		change := c.Delta[pos]
		switch change.GetOperation() {
		case scylla_cdc.Update:
			r.processUpdate(timestamp, change)
			pos++

		case scylla_cdc.Insert:
			r.processInsert(timestamp, change)
			pos++

		case scylla_cdc.RowDelete:
			r.processRowDelete(timestamp, change)
			pos++

		case scylla_cdc.PartitionDelete:
			r.processPartitionDelete(timestamp, change)
			pos++

		case scylla_cdc.RangeDeleteStartInclusive, scylla_cdc.RangeDeleteStartExclusive:
			// TODO: Check that we aren't at the end?
			start := change
			end := c.Delta[pos+1]
			r.processRangeDelete(timestamp, start, end)
			pos += 2

		default:
			panic("unsupported operation: " + change.GetOperation().String())
		}
	}
}

func (r *DeltaReplicator) processUpdate(timestamp int64, c *scylla_cdc.ChangeRow) {

	r.processInsertOrUpdate(timestamp, false, c)
}

func (r *DeltaReplicator) processInsert(timestamp int64, c *scylla_cdc.ChangeRow) {
	r.processInsertOrUpdate(timestamp, true, c)
}

func (r *DeltaReplicator) processInsertOrUpdate(timestamp int64, isInsert bool, c *scylla_cdc.ChangeRow) {
	overwriteVals := make([]interface{}, 0, len(r.allColumns)+1)

	if !isInsert {
		// In UPDATE, the TTL goes first
		overwriteVals = append(overwriteVals, c.GetTTL())
	}

	batch := gocql.NewBatch(gocql.UnloggedBatch)

	for i, colName := range r.otherColumns {
		typ := r.otherColumnTypes[i]
		isNonFrozenCollection := !typ.IsFrozen() && typ.Type().IsCollection()

		v, hasV := c.GetValue(colName)
		isDeleted := c.IsDeleted(colName)

		if !isNonFrozenCollection {
			if isDeleted {
				overwriteVals = append(overwriteVals, nil)
			} else {
				if hasV {
					overwriteVals = append(overwriteVals, v)
				} else {
					overwriteVals = append(overwriteVals, gocql.UnsetValue)
				}
			}
		} else {
			pcuq := &r.perColumnUpdateQueries[i]
			rDelEls := reflect.ValueOf(c.GetDeletedElements(colName))
			delElsLen := rDelEls.Len()
			if typ.Type() == TypeList {
				overwriteVals = append(overwriteVals, gocql.UnsetValue)
				if isDeleted {
					// The list may be overwritten
					// We realize it in two operations: clear + append
					//
					// We can't just do UPDATE SET l = [...],
					// because we need to precisely control timestamps
					// of the list cells. This can be done only by
					// UPDATE SET l[SCYLLA_TIMEUUID_LIST_INDEX(?)] = ?,
					// which is equivalent to an append of one cell.
					// Hence, the need for clear + append.
					//
					// We clear using a timestamp one-less-than the real
					// timestamp of the write. This is what Cassandra/Scylla
					// does internally, so it's OK to for us to do that.

					clearVals := make([]interface{}, 2, len(r.pkColumns)+len(r.ckColumns)+2)
					clearVals[0] = c.GetTTL()
					clearVals[1] = timestamp
					if hasV {
						clearVals[1] = timestamp - 1
					}
					clearVals = appendKeyValuesToBind(clearVals, r.pkColumns, c)
					clearVals = appendKeyValuesToBind(clearVals, r.ckColumns, c)
					if debugQueries {
						fmt.Println(pcuq.remove)
						fmt.Println(clearVals...)
					}
					batch.Query(pcuq.remove, clearVals...)
				}
				if hasV {
					// Append to the list
					rv := reflect.ValueOf(v)
					iter := rv.MapRange()
					for iter.Next() {
						key := iter.Key().Interface()
						val := iter.Value().Interface()

						setVals := make([]interface{}, 3, len(r.pkColumns)+len(r.ckColumns)+3)
						setVals[0] = c.GetTTL()
						setVals[1] = key
						setVals[2] = val
						setVals = appendKeyValuesToBind(setVals, r.pkColumns, c)
						setVals = appendKeyValuesToBind(setVals, r.ckColumns, c)
						if debugQueries {
							fmt.Println(pcuq.add)
							fmt.Println(setVals...)
						}
						batch.Query(pcuq.add, setVals...)
					}
				}
				if delElsLen != 0 {
					// Remove from the list
					for i := 0; i < delElsLen; i++ {
						setVals := make([]interface{}, 3, len(r.pkColumns)+len(r.ckColumns)+3)
						setVals[0] = c.GetTTL()
						setVals[1] = rDelEls.Index(i).Interface()
						setVals[2] = nil
						setVals = appendKeyValuesToBind(setVals, r.pkColumns, c)
						setVals = appendKeyValuesToBind(setVals, r.ckColumns, c)
						if debugQueries {
							fmt.Println(pcuq.add)
							fmt.Println(setVals...)
						}
						batch.Query(pcuq.add, setVals...)
					}
				}
			} else {
				if hasV {
					if isDeleted {
						// The value of the collection is being overwritten
						overwriteVals = append(overwriteVals, v)
					} else {
						// We are appending to the collection
						addVals := make([]interface{}, 2, len(r.pkColumns)+len(r.ckColumns)+2)
						addVals[0] = c.GetTTL()
						addVals[1] = v
						addVals = appendKeyValuesToBind(addVals, r.pkColumns, c)
						addVals = appendKeyValuesToBind(addVals, r.ckColumns, c)
						if debugQueries {
							fmt.Println(pcuq.add)
							fmt.Println(addVals...)
						}
						batch.Query(pcuq.add, addVals...)
						overwriteVals = append(overwriteVals, gocql.UnsetValue)
					}
				} else if isDeleted {
					// Collection is being reset to nil
					overwriteVals = append(overwriteVals, nil)
				} else {
					overwriteVals = append(overwriteVals, gocql.UnsetValue)
				}
				if delElsLen != 0 {
					subVals := make([]interface{}, 2, len(r.pkColumns)+len(r.ckColumns)+2)
					subVals[0] = c.GetTTL()
					subVals[1] = rDelEls.Interface()
					subVals = appendKeyValuesToBind(subVals, r.pkColumns, c)
					subVals = appendKeyValuesToBind(subVals, r.ckColumns, c)
					if debugQueries {
						fmt.Println(pcuq.remove)
						fmt.Println(subVals...)
					}
					batch.Query(pcuq.remove, subVals...)
				}
			}
		}
	}

	var doRegularUpdate bool

	if isInsert {
		// In case of INSERT, we MUST perform the insert, even if only collections
		// are updated. This is because INSERT, contrary to UPDATE, sets the
		// row marker.
		doRegularUpdate = true
	} else {
		for _, v := range overwriteVals {
			if v != gocql.UnsetValue {
				doRegularUpdate = true
				break
			}
		}
	}

	if doRegularUpdate {
		overwriteVals = appendKeyValuesToBind(overwriteVals, r.pkColumns, c)
		overwriteVals = appendKeyValuesToBind(overwriteVals, r.ckColumns, c)

		if isInsert {
			// In INSERT, the TTL goes at the end
			overwriteVals = append(overwriteVals, c.GetTTL())
		}

		if isInsert {
			if debugQueries {
				fmt.Println(r.insertQueryStr)
				fmt.Println(overwriteVals...)
			}
			batch.Query(r.insertQueryStr, overwriteVals...)
		} else {
			if debugQueries {
				fmt.Println(r.updateQueryStr)
				fmt.Println(overwriteVals...)
			}
			batch.Query(r.updateQueryStr, overwriteVals...)
		}
	}

	batch.SetConsistency(r.consistency)
	batch.WithTimestamp(timestamp)

	err := r.session.ExecuteBatch(batch)
	if err != nil {
		typ := "update"
		if isInsert {
			typ = "insert"
		}
		fmt.Printf("ERROR while trying to %s: %s\n", typ, err)
	}
}

func (r *DeltaReplicator) processRowDelete(timestamp int64, c *scylla_cdc.ChangeRow) {
	// TODO: Cache vals?
	vals := make([]interface{}, 0, len(r.pkColumns)+len(r.ckColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)
	vals = appendKeyValuesToBind(vals, r.ckColumns, c)

	if debugQueries {
		fmt.Println(r.rowDeleteQueryStr)
		fmt.Println(vals...)
	}

	// TODO: Propagate errors
	err := r.session.
		Query(r.rowDeleteQueryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to delete row: %s\n", err)
	}
}

func (r *DeltaReplicator) processPartitionDelete(timestamp int64, c *scylla_cdc.ChangeRow) {
	// TODO: Cache vals?
	vals := make([]interface{}, 0, len(r.pkColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)

	if debugQueries {
		fmt.Println(r.partitionDeleteQueryStr)
		fmt.Println(vals...)
	}

	// TODO: Propagate errors
	err := r.session.
		Query(r.partitionDeleteQueryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to delete partition: %s\n", err)
	}
}

func (r *DeltaReplicator) processRangeDelete(timestamp int64, start, end *scylla_cdc.ChangeRow) {
	// TODO: Cache vals?
	vals := make([]interface{}, 0, len(r.pkColumns)+len(r.ckColumns)+1)
	vals = appendKeyValuesToBind(vals, r.pkColumns, start)

	// Find the right query to use
	var (
		prevRight         interface{}
		left, right       interface{}
		hasLeft, hasRight bool
		baseIdx           int = -1
	)

	// TODO: Explain what this loop does
	for i, ckCol := range r.ckColumns {
		left, hasLeft = start.GetValue(ckCol)
		right, hasRight = end.GetValue(ckCol)

		if hasLeft {
			if hasRight {
				// Has both left and right
				// It's either a delete bounded from two sides, or it's an
				// equality condition
				// If it's the last ck column or the next ck column will be null
				// in both start and end, then it's an two-sided bound
				// If not, it's an equality condition
				prevRight = right
				vals = append(vals, left)
				continue
			} else {
				// Bounded from the left
				vals = append(vals, left)
				baseIdx = i
				break
			}
		} else {
			if hasRight {
				// Bounded from the right
				vals = append(vals, right)
				baseIdx = i
				break
			} else {
				// The previous column was a two-sided bound
				// In previous iteration, we appended the left bound
				// Append the right bound
				vals = append(vals, prevRight)
				hasLeft = true
				hasRight = true
				baseIdx = i - 1
				break
			}
		}
	}

	if baseIdx == -1 {
		// It's a two-sided bound
		vals = append(vals, prevRight)
		baseIdx = len(r.ckColumns) - 1
	}

	// Magic... TODO: Make it more clear
	leftOff := 2
	if hasLeft {
		leftOff = int(start.GetOperation() - scylla_cdc.RangeDeleteStartInclusive)
	}
	rightOff := 2
	if hasRight {
		rightOff = int(end.GetOperation() - scylla_cdc.RangeDeleteEndInclusive)
	}
	queryIdx := 8*baseIdx + leftOff + 3*rightOff
	queryStr := r.rangeDeleteQueryStrs[queryIdx]

	if debugQueries {
		fmt.Println(queryStr)
		fmt.Println(vals...)
	}

	// TODO: Propagate errors
	err := r.session.
		Query(queryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to delete range: %s\n", err)
	}
}

func makeBindMarkerAssignmentList(columnNames []string) []string {
	assignments := make([]string, 0, len(columnNames))
	for _, name := range columnNames {
		assignments = append(assignments, name+" = ?")
	}
	return assignments
}

func makeBindMarkerAssignments(columnNames []string, sep string) string {
	assignments := makeBindMarkerAssignmentList(columnNames)
	return strings.Join(assignments, sep)
}

func appendKeyValuesToBind(
	vals []interface{},
	names []string,
	c *scylla_cdc.ChangeRow,
) []interface{} {
	// No need to handle non-frozen lists here, because they can't appear
	// in either partition or clustering key
	for _, name := range names {
		v, ok := c.GetValue(name)
		if !ok {
			v = gocql.UnsetValue
		}
		vals = append(vals, v)
	}
	return vals
}

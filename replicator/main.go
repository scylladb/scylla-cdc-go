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
	"time"

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
		ConfidenceWindowSize:   0,
		ChangeAgeLimit:         24 * time.Hour,
		QueryTimeWindowSize:    24 * time.Hour,
		PostEmptyQueryDelay:    15 * time.Second,
		PostNonEmptyQueryDelay: 5 * time.Second,
		PostFailedQueryDelay:   5 * time.Second,
	}

	reader, err := MakeReplicator(
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

	log.Println("quitting")
}

func MakeReplicator(
	source, destination string,
	tableNames []string,
	advancedParams *scylla_cdc.AdvancedReaderConfig,
	consistency gocql.Consistency,
) (*scylla_cdc.Reader, error) {
	// Configure a session for the destination cluster
	destinationCluster := gocql.NewCluster(destination)
	destinationSession, err := destinationCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	tracker := scylla_cdc.NewClusterStateTracker(gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()))

	// Configure a session
	cluster := gocql.NewCluster(source)
	cluster.PoolConfig.HostSelectionPolicy = tracker
	session, err := cluster.CreateSession()
	if err != nil {
		destinationSession.Close()
		return nil, err
	}

	factory := &replicatorFactory{
		destinationSession: destinationSession,
		consistency:        consistency,
	}

	// Configuration for the CDC reader
	cfg := scylla_cdc.NewReaderConfig(
		session,
		factory,
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
		return nil, err
	}

	// TODO: source and destination sessions are leaking
	return reader, nil
}

type replicatorFactory struct {
	destinationSession *gocql.Session
	consistency        gocql.Consistency
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

	return NewDeltaReplicator(rf.destinationSession, tmeta, rf.consistency), nil
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

func NewDeltaReplicator(session *gocql.Session, meta *gocql.TableMetadata, consistency gocql.Consistency) *DeltaReplicator {
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
	assignments := r.makeBindMarkerAssignments(r.otherColumns, ", ")

	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)
	conditions := r.makeBindMarkerAssignments(keyColumns, " AND ")

	r.updateQueryStr = fmt.Sprintf(
		"UPDATE %s USING TTL ? SET %s WHERE %s",
		r.tableName,
		assignments,
		conditions,
	)

	// Compute per-column update queries (only for non-frozen collections)
	queries := make([]updateQuerySet, len(r.otherColumns))
	for i, colName := range r.otherColumns {
		typ := r.columnTypes[colName]
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

	valueMarkers := []string{}
	for _, colName := range r.otherColumns {
		valueMarkers = append(valueMarkers, makeBindMarkerForType(r.columnTypes[colName]))
	}
	for i := 0; i < len(r.pkColumns)+len(r.ckColumns); i++ {
		valueMarkers = append(valueMarkers, "?")
	}
	valueMarkersTuple := "(" + strings.Join(valueMarkers, ", ") + ")"

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

func (r *DeltaReplicator) computeRangeDeleteQueries() {
	r.rangeDeleteQueryStrs = make([]string, 0, 8*len(r.ckColumns))

	prefix := fmt.Sprintf("DELETE FROM %s WHERE ", r.tableName)
	eqConds := r.makeBindMarkerAssignmentList(r.pkColumns)

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

func (r *DeltaReplicator) Consume(c scylla_cdc.Change) error {
	timestamp := c.GetCassandraTimestamp()
	pos := 0

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

	return nil
}

func (r *DeltaReplicator) End() {
	// TODO: Take a snapshot here
}

func (r *DeltaReplicator) processUpdate(timestamp int64, c *scylla_cdc.ChangeRow) error {
	return r.processInsertOrUpdate(timestamp, false, c)
}

func (r *DeltaReplicator) processInsert(timestamp int64, c *scylla_cdc.ChangeRow) error {
	return r.processInsertOrUpdate(timestamp, true, c)
}

func (r *DeltaReplicator) processInsertOrUpdate(timestamp int64, isInsert bool, c *scylla_cdc.ChangeRow) error {
	overwriteVals := make([]interface{}, 0, len(r.allColumns)+1)

	if !isInsert {
		// In UPDATE, the TTL goes first
		overwriteVals = append(overwriteVals, c.GetTTL())
	}

	batch := gocql.NewBatch(gocql.UnloggedBatch)

	for i, colName := range r.otherColumns {
		typ := r.columnTypes[colName]
		isNonFrozenCollection := !typ.IsFrozen() && typ.Type().IsCollection()

		v, hasV := c.GetValue(colName)
		isDeleted := c.IsDeleted(colName)

		if !isNonFrozenCollection {
			if typ.Type() == TypeTuple {
				tupleTyp := typ.Unfrozen().(*TupleType)
				for i := range tupleTyp.Elements {
					v, hasV := c.GetValue(gocql.TupleColumnName(colName, i))
					if hasV {
						overwriteVals = append(overwriteVals, v)
					} else if isDeleted {
						overwriteVals = append(overwriteVals, nil)
					} else {
						overwriteVals = append(overwriteVals, gocql.UnsetValue)
					}
				}
			} else {
				if isDeleted {
					overwriteVals = append(overwriteVals, nil)
				} else {
					if hasV {
						overwriteVals = append(overwriteVals, v)
					} else {
						overwriteVals = append(overwriteVals, gocql.UnsetValue)
					}
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

	return err
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

	return err
}

func (r *DeltaReplicator) processPartitionDelete(timestamp int64, c *scylla_cdc.ChangeRow) error {
	// TODO: Cache vals?
	vals := make([]interface{}, 0, len(r.pkColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)

	if debugQueries {
		fmt.Println(r.partitionDeleteQueryStr)
		fmt.Println(vals...)
	}

	err := r.session.
		Query(r.partitionDeleteQueryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to delete partition: %s\n", err)
	}

	// TODO: Retries
	return err
}

func (r *DeltaReplicator) processRangeDelete(timestamp int64, start, end *scylla_cdc.ChangeRow) error {
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

	err := r.session.
		Query(queryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to delete range: %s\n", err)
	}

	// TODO: Retries
	return err
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
	for _, typ := range tupleTyp.Elements {
		vals = append(vals, makeBindMarkerForType(typ))
	}
	return "(" + strings.Join(vals, ", ") + ")"
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

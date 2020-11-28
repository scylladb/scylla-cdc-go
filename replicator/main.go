package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/gocql/gocql"
	scylla_cdc "github.com/piodul/scylla-cdc-go"
)

// TODO: Escape field names?

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

	// Configure a session for the destination cluster
	destinationCluster := gocql.NewCluster(destination)
	destinationSession, err := destinationCluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer destinationSession.Close()

	kmeta, err := destinationSession.KeyspaceMetadata(keyspace)
	if err != nil {
		log.Fatal(err)
	}
	tmeta, ok := kmeta.Tables[table]
	if !ok {
		log.Fatalf("table %s does not exist", table)
	}
	replicator := NewDeltaReplicator(destinationSession, tmeta)

	tracker := scylla_cdc.NewClusterStateTracker(gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()))

	// Configure a session first
	cluster := gocql.NewCluster(source)
	cluster.PoolConfig.HostSelectionPolicy = tracker
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Configuration for the CDC reader
	cfg := &scylla_cdc.ReaderConfig{
		Session:             session,
		Consistency:         gocql.Quorum,
		LogTableName:        keyspace + "." + table + "_scylla_cdc_log",
		ChangeConsumer:      replicator,
		ClusterStateTracker: tracker,

		Logger: log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile),
	}

	reader, err := scylla_cdc.NewReader(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// React to Ctrl+C signal, and stop gracefully after the first signal
	// Second signal exits the process
	signalC := make(chan os.Signal)
	go func() {
		<-signalC
		reader.Stop()

		<-signalC
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	if err := reader.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

type DeltaReplicator struct {
	session     *gocql.Session
	tableName   string
	consistency gocql.Consistency

	pkColumns    []string
	ckColumns    []string
	otherColumns []string
	allColumns   []string

	insertQueryStr          string
	updateQueryStr          string
	rowDeleteQueryStr       string
	partitionDeleteQueryStr string
	rangeDeleteQueryStrs    []string
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

	dr := &DeltaReplicator{
		session:     session,
		tableName:   meta.Keyspace + "." + meta.Name,
		consistency: gocql.Quorum,

		pkColumns:    pkColumns,
		ckColumns:    ckColumns,
		otherColumns: otherColumns,
		allColumns:   append([]string{}, meta.OrderedColumns...),
	}

	dr.computeInsertQuery()
	dr.computeUpdateQuery()
	dr.computeRowDeleteQuery()
	dr.computePartitionDeleteQuery()
	dr.computeRangeDeleteQueries()

	return dr
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

func (r *DeltaReplicator) computeUpdateQuery() {
	assignments := makeBindMarkerAssignments(r.otherColumns)

	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)
	conditions := makeBindMarkerAssignments(keyColumns)

	r.updateQueryStr = fmt.Sprintf(
		"UPDATE %s USING TTL ? SET %s WHERE %s",
		r.tableName,
		assignments,
		conditions,
	)
}

func (r *DeltaReplicator) computeRowDeleteQuery() {
	keyColumns := append(append([]string{}, r.pkColumns...), r.ckColumns...)

	r.rowDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		makeBindMarkerAssignments(keyColumns),
	)
}

func (r *DeltaReplicator) computePartitionDeleteQuery() {
	r.partitionDeleteQueryStr = fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.tableName,
		makeBindMarkerAssignments(r.pkColumns),
	)
}

func (r *DeltaReplicator) computeRangeDeleteQueries() {
	r.rangeDeleteQueryStrs = make([]string, 0, 8*len(r.ckColumns))

	prefix := fmt.Sprintf("DELETE FROM %s WHERE ", r.tableName)
	eqConds := makeBindMarkerAssignmentList(r.pkColumns)

	for _, ckCol := range r.ckColumns {
		for typ := 0; typ < 8; typ++ {
			startOp := [3]string{">=", "=", ""}[typ%3]
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

func (r *DeltaReplicator) Consume(c scylla_cdc.Change) {
	timestamp := c.GetCassandraTimestamp()
	pos := 0

	for pos < len(c.Delta) {
		change := c.Delta[pos]
		switch change.GetOperation() {
		case scylla_cdc.Insert:
			r.processInsert(timestamp, change)
			pos++

		case scylla_cdc.Update:
			r.processUpdate(timestamp, change)
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

func (r *DeltaReplicator) processInsert(timestamp int64, c *scylla_cdc.ChangeRow) {
	// TODO: Cache vals?
	vals := make([]interface{}, 0, len(r.allColumns)+1)
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)
	vals = appendKeyValuesToBind(vals, r.ckColumns, c)
	vals = appendRegularValuesToBind(vals, r.otherColumns, c)
	vals = append(vals, c.GetTTL())

	// TODO: Propagate errors
	err := r.session.
		Query(r.insertQueryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to insert: %s\n", err)
	}
}

func (r *DeltaReplicator) processUpdate(timestamp int64, c *scylla_cdc.ChangeRow) {
	// TODO: Cache vals?
	// TODO: Collection updates
	// Those should be done using batches, I think
	vals := make([]interface{}, 1, len(r.allColumns)+1)
	vals[0] = c.GetTTL()
	vals = appendRegularValuesToBind(vals, r.otherColumns, c)
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)
	vals = appendKeyValuesToBind(vals, r.ckColumns, c)

	// TODO: Propagate errors
	err := r.session.
		Query(r.updateQueryStr, vals...).
		Consistency(r.consistency).
		Idempotent(true).
		WithTimestamp(timestamp).
		Exec()
	if err != nil {
		fmt.Printf("ERROR while trying to update: %s\n", err)
	}
}

func (r *DeltaReplicator) processRowDelete(timestamp int64, c *scylla_cdc.ChangeRow) {
	// TODO: Cache vals?
	vals := make([]interface{}, 0, len(r.pkColumns)+len(r.ckColumns))
	vals = appendKeyValuesToBind(vals, r.pkColumns, c)
	vals = appendKeyValuesToBind(vals, r.ckColumns, c)

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

func makeBindMarkerAssignments(columnNames []string) string {
	assignments := makeBindMarkerAssignmentList(columnNames)
	return strings.Join(assignments, " AND ")
}

func appendKeyValuesToBind(
	vals []interface{},
	names []string,
	c *scylla_cdc.ChangeRow,
) []interface{} {
	for _, name := range names {
		v, ok := c.GetValue(name)
		if !ok {
			v = gocql.UnsetValue
		}
		vals = append(vals, v)
	}
	return vals
}

func appendRegularValuesToBind(
	vals []interface{},
	names []string,
	c *scylla_cdc.ChangeRow,
) []interface{} {
	for _, name := range names {
		v, ok := c.GetValue(name)
		if !ok {
			if c.IsDeleted(name) {
				v = nil
			} else {
				v = gocql.UnsetValue
			}
		}
		vals = append(vals, v)
	}
	return vals
}

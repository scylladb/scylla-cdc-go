package scylla_cdc

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

type OperationType int8

const (
	PreImage                  OperationType = 0
	Update                                  = 1
	Insert                                  = 2
	RowDelete                               = 3
	PartitionDelete                         = 4
	RangeDeleteStartInclusive               = 5
	RangeDeleteStartExclusive               = 6
	RangeDeleteEndInclusive                 = 7
	RangeDeleteEndExclusive                 = 8
	PostImage                               = 9
)

func (ot OperationType) String() string {
	switch ot {
	case PreImage:
		return "PREIMAGE"
	case Update:
		return "UPDATE"
	case Insert:
		return "INSERT"
	case RowDelete:
		return "ROW_DELETE"
	case PartitionDelete:
		return "PARTITION_DELETE"
	case RangeDeleteStartInclusive:
		return "RANGE_DELETE_START_INCLUSIVE"
	case RangeDeleteStartExclusive:
		return "RANGE_DELETE_START_EXCLUSIVE"
	case RangeDeleteEndInclusive:
		return "RANGE_DELETE_END_INCLUSIVE"
	case RangeDeleteEndExclusive:
		return "RANGE_DELETE_END_EXCLUSIVE"
	case PostImage:
		return "POSTIMAGE"
	default:
		return "wrong OperationType"
	}
}

type Change struct {
	StreamID  []byte
	Time      gocql.UUID
	Preimage  []*ChangeRow
	Delta     []*ChangeRow
	Postimage []*ChangeRow
}

// GetCassandraTimestamp returns a timestamp of the operation
// suitable to put as a TIMESTAMP parameter to a DML statement
// (INSERT, UPDATE, DELETE)
func (c *Change) GetCassandraTimestamp() int64 {
	return timeuuidToTimestamp(c.Time)
}

// ChangeRow corresponds to a single row from the cdc log
type ChangeRow struct {
	data    map[string]interface{}
	cdcCols cdcChangeCols
}

type cdcStreamCols struct {
	streamID []byte
	time     gocql.UUID
}

type cdcChangeCols struct {
	batchSeqNo int32
	operation  int8
	ttl        int64
	endOfBatch bool
}

// GetOperation returns the type of operation this change represents
func (c *ChangeRow) GetOperation() OperationType {
	return OperationType(c.cdcCols.operation)
}

// GetTTL returns 0 if TTL was not set for this operation
func (c *ChangeRow) GetTTL() int64 {
	return c.cdcCols.ttl
}

// GetValue returns value that was assigned to this specific column
func (c *ChangeRow) GetValue(columnName string) (interface{}, bool) {
	v, ok := c.data[columnName]
	return v, ok
}

// IsDeleted returns a boolean indicating if given column was set to null.
// This only works for clustering columns.
func (c *ChangeRow) IsDeleted(columnName string) bool {
	return c.data["cdc$deleted_"+columnName].(bool)
}

// GetDeletedElements returns which elements were deleted from the non-atomic column.
// This function works only for non-atomic columns
func (c *ChangeRow) GetDeletedElements(columnName string) interface{} {
	return c.data["cdc$deleted_elements_"+columnName]
}

func (c *ChangeRow) String() string {
	// TODO: This doesn't work correctly because null columns are not inserted
	// to the map

	var b strings.Builder
	b.WriteString(OperationType(c.cdcCols.operation).String())
	b.WriteString(" ")
	b.WriteString(strconv.FormatInt(c.cdcCols.ttl, 10))
	b.WriteString(" -> {")
	first := true

	// Sort field names
	sortedFieldNames := make([]string, 0, len(c.data))
	for k := range c.data {
		if strings.HasPrefix(k, "cdc$deleted_") {
			continue
		}
		sortedFieldNames = append(sortedFieldNames, k)
	}

	// Copy field names, and included cdc$deleted_ columns
	sort.Strings(sortedFieldNames)
	ks := make([]string, 0, len(c.data))
	for _, k := range sortedFieldNames {
		ks = append(ks, k)
		deleted := "cdc$deleted_" + k
		deletedElements := "cdc$deleted_elements_" + k
		if _, hasDeleted := c.data[deleted]; hasDeleted {
			ks = append(ks, deleted)
		}
		if _, hasDeletedElements := c.data[deletedElements]; hasDeletedElements {
			ks = append(ks, deletedElements)
		}
	}

	// Print columns in order
	for _, k := range ks {
		v := c.data[k]
		if !first {
			b.WriteString(" ")
		}
		first = false
		b.WriteString(k)
		b.WriteString(":")
		if strings.HasPrefix(k, "cdc$deleted_") {
			b.WriteString(fmt.Sprintf("%v", v))
		} else {
			// Should be a pointer
			v := reflect.ValueOf(v)
			if v.IsNil() {
				b.WriteString("nil")
			} else {
				b.WriteString(fmt.Sprintf("%v", reflect.Indirect(v).Interface()))
			}
		}
	}
	b.WriteString("}")
	return b.String()
}

type ChangeConsumer interface {
	Consume(Change)
}

type ChangeConsumerFunc func(Change)

func (ccf ChangeConsumerFunc) Consume(c Change) {
	ccf(c)
}

// An adapter over gocql.Iterator
type changeRowIterator struct {
	iter         *gocql.Iter
	columnNames  []string
	columnValues []interface{}

	cdcStreamCols cdcStreamCols
	cdcChangeCols cdcChangeCols
}

func newChangeRowIterator(iter *gocql.Iter) (*changeRowIterator, error) {
	// TODO: Check how costly is the reflection here
	// We could amortize the cost by preparing the dataFields only at the
	// beginning of the iteration, and change them only if the fields
	// have changed
	// This possibility should be looked into

	// Prepare row data
	rowData, err := iter.RowData()
	if err != nil {
		return nil, err
	}
	columnNames := rowData.Columns
	columnValues := rowData.Values

	ci := &changeRowIterator{
		iter:         iter,
		columnNames:  columnNames,
		columnValues: columnValues,
	}

	for i := 0; i < len(columnNames); i++ {
		columnName := columnNames[i]
		clearColumnName := true
		switch columnName {
		case "cdc$stream_id":
			columnValues[i] = &ci.cdcStreamCols.streamID
		case "cdc$time":
			columnValues[i] = &ci.cdcStreamCols.time
		case "cdc$batch_seq_no":
			columnValues[i] = &ci.cdcChangeCols.batchSeqNo
		case "cdc$ttl":
			columnValues[i] = &ci.cdcChangeCols.ttl
		case "cdc$operation":
			columnValues[i] = &ci.cdcChangeCols.operation
		case "cdc$end_of_batch":
			columnValues[i] = &ci.cdcChangeCols.endOfBatch

		default:
			if !strings.HasPrefix(columnName, "cdc$deleted_") {
				// All non-cdc fields should be nullable
				columnValues[i] = reflect.New(reflect.TypeOf(columnValues[i])).Interface()
			}
			clearColumnName = false
		}

		if clearColumnName {
			// Don't put always-occurring cdc columns into the map
			columnNames[i] = ""
		}
	}

	return ci, nil
}

func (ci *changeRowIterator) Next() (cdcStreamCols, *ChangeRow) {
	if !ci.iter.Scan(ci.columnValues...) {
		return cdcStreamCols{}, nil
	}

	// Make a copy so that the Change object can be used safely after Next() is called again
	// TODO: Maybe we can omit copying here? We could re-use a single map
	// But it would require entrusting the user with performing a necessary copy
	// if they want to preserve data across Next() calls
	// TODO: Can we design an interface which scans into user-provided struct?
	change := &ChangeRow{
		data:    make(map[string]interface{}, len(ci.columnValues)-6),
		cdcCols: ci.cdcChangeCols,
	}
	for i, name := range ci.columnNames {
		if name != "" {
			v, notNull := maybeDereferenceTwice(ci.columnValues[i])
			if notNull {
				change.data[name] = v
			}
		}
	}
	return ci.cdcStreamCols, change
}

func (ci *changeRowIterator) Close() error {
	return ci.iter.Close()
}

func maybeDereferenceTwice(i interface{}) (interface{}, bool) {
	v := reflect.Indirect(reflect.ValueOf(i))
	if v.Kind() != reflect.Ptr {
		return v.Interface(), true
	}
	if v.IsNil() {
		return nil, false
	}
	return reflect.Indirect(v).Interface(), true
}

func timeuuidToTimestamp(from gocql.UUID) int64 {
	return (from.Timestamp() - 0x01b21dd213814000) / 10
}

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
		return "(wrong OperationType)"
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

	colInfos []gocql.ColumnInfo
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

// ScalarChange represents a change to a column of native type, or a frozen
// type.
type ScalarChange struct {
	// Value contains the scalar value of the column.
	// If the column was not changed or was deleted, it will be nil.
	//
	// Type: T or nil interface.
	Value interface{}

	// IsDeleted tells if this column was set to NULL by this change.
	IsDeleted bool
}

// ListChange represents a change to a column
type ListChange struct {
	// AppendedElements contains values appended to the list in the form
	// of map from cell timestamps to values.
	// If there were no appended values, it can be nil.
	// For more information about how to interpret it, see README and examples.
	// TODO: Actually document it in README and examples
	//
	// Type: map[gocql.UUID]T, if there were any elements appended, or nil
	// interface.
	AppendedElements interface{}

	// TODO: Document, it's not really clear what it is without context,
	// and it's not very easy to use
	//
	// Type: []gocql.UUID if there were any elements removed, or nil interface.
	RemovedElements []gocql.UUID

	// IsReset tells if the list value was overwritten instead of being
	// appended to or removed from. If it's true, than AppendedValue will
	// contain the new state of the list (which can be NULL).
	IsReset bool
}

// SetChange represents a change to a column of type set<T>.
type SetChange struct {
	// AddedElements contains a slice of values which were added to the set
	// by the operation. If there were any values added, it will contain
	// a slice of form []T, where T is gocql's representation of the element
	// type. If there were no values added, this field can contain a nil
	// interface.
	//
	// Type: []T, if there were any elements added, or nil interface.
	AddedElements interface{}

	// RemovedElements contains a slice of values which were removed from the set
	// by the operation. Like AddedValues, it's either a slice or a nil
	// interface.
	// Please not that if the operation overwrote the old value of the set
	// instead of adding/removing elements, this field _will be nil_.
	// Instead, IsReset field will be set, and AddedValues will contain
	// the new state of the set.
	//
	// Type: []T, if there were any elements removed, or nil interface.
	RemovedElements interface{}

	// IsReset tells if the set value was overwritten instead of being
	// appended to or removed from. If it's true, than AppendedValue will
	// contain the new state of the set (which can be NULL).
	IsReset bool
}

// MapChange represents a change to a column of type map<K, V>.
type MapChange struct {
	// AddedElements contains a map of elements which were added to the map
	// by the operation.
	//
	// Type: map[K]V, if there were any added values, or nil interface.
	AddedElements interface{}

	// RemovedElements contains a slice of keys which were removed from the map
	// by the operation.
	// Please not that if the operation overwrote the old value of the map
	// instead of adding/removing elements, this field _will be nil_.
	// Instead, IsReset field will be set, and AddedValues will contain
	// the new state of the map.
	//
	// Type: []K, if there were any indices removed, or nil interface.
	RemovedElements interface{}

	// IsReset tells if the map value was overwritten instead of being
	// appended to or removed from. If it's true, than AppendedValue will
	// contain the new state of the map (which can be NULL).
	IsReset bool
}

// TODO: document UDTChange
type UDTChange struct {
	AddedFields map[string]interface{}

	RemovedFields []int16

	IsReset bool
}

func (c *ChangeRow) GetScalarChange(column string) ScalarChange {
	v, _ := c.GetValue(column)
	isDeleted, _ := c.IsDeleted(column)
	return ScalarChange{
		Value:     v,
		IsDeleted: isDeleted,
	}
}

func (c *ChangeRow) GetListChange(column string) ListChange {
	// TODO: Warn about usage with frozen lists
	// Maybe convert lists to frozen form?
	// There isn't really a way to do so, though...
	v, _ := c.GetValue(column)
	isDeleted, _ := c.IsDeleted(column)
	deletedElements, _ := c.GetDeletedElements(column)
	typedDeletedElements, _ := deletedElements.([]gocql.UUID)
	return ListChange{
		AppendedElements: v,
		RemovedElements:  typedDeletedElements,
		IsReset:          isDeleted,
	}
}

func (c *ChangeRow) GetSetChange(column string) SetChange {
	v, _ := c.GetValue(column)
	isDeleted, _ := c.IsDeleted(column)
	deletedElements, _ := c.GetDeletedElements(column)
	return SetChange{
		AddedElements:   v,
		RemovedElements: deletedElements,
		IsReset:         isDeleted,
	}
}

func (c *ChangeRow) GetMapChange(column string) MapChange {
	v, _ := c.GetValue(column)
	isDeleted, _ := c.IsDeleted(column)
	deletedElements, _ := c.GetDeletedElements(column)
	return MapChange{
		AddedElements:   v,
		RemovedElements: deletedElements,
		IsReset:         isDeleted,
	}
}

func (c *ChangeRow) GetUDTChange(column string) UDTChange {
	v, _ := c.GetValue(column)
	typedV, _ := v.(map[string]interface{})
	isDeleted, _ := c.IsDeleted(column)
	deletedElements, _ := c.GetDeletedElements(column)
	typedDeletedElements, _ := deletedElements.([]int16)
	udtC := UDTChange{
		AddedFields:   typedV,
		RemovedFields: typedDeletedElements,
		IsReset:       isDeleted,
	}

	return udtC
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
func (c *ChangeRow) IsDeleted(columnName string) (bool, bool) {
	v, ok := c.data["cdc$deleted_"+columnName]
	if !ok {
		return false, false
	}
	return v.(bool), true
}

// GetDeletedElements returns which elements were deleted from the non-atomic column.
// This function works only for non-atomic columns
func (c *ChangeRow) GetDeletedElements(columnName string) (interface{}, bool) {
	v, ok := c.data["cdc$deleted_elements_"+columnName]
	return v, ok
}

// Columns returns information about data columns in the cdc log table (without those with "cdc$" prefix)
func (c *ChangeRow) Columns() []gocql.ColumnInfo {
	return c.colInfos
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
		v, present := c.data[k]
		if !first {
			b.WriteString(" ")
		}
		first = false
		b.WriteString(k)
		b.WriteString(":")
		if strings.HasPrefix(k, "cdc$deleted_") {
			b.WriteString(fmt.Sprintf("%v", v))
		} else {
			if present {
				b.WriteString(fmt.Sprintf("%v", v))
			} else {
				b.WriteString("nil")
			}
		}
	}
	b.WriteString("}")
	return b.String()
}

type CreateChangeConsumerInput struct {
	TableName string
	StreamIDs []StreamID
}

type ChangeConsumerFactory interface {
	CreateChangeConsumer(input CreateChangeConsumerInput) (ChangeConsumer, error)
}

type ChangeConsumer interface {
	End()
	Consume(change Change) error
}

func MakeChangeConsumerFactoryFromFunc(f ChangeConsumerFunc) ChangeConsumerFactory {
	return &changeConsumerFuncInstanceFactory{f}
}

type changeConsumerFuncInstanceFactory struct {
	f ChangeConsumerFunc
}

func (ccfif *changeConsumerFuncInstanceFactory) CreateChangeConsumer(input CreateChangeConsumerInput) (ChangeConsumer, error) {
	return &changeConsumerFuncInstance{
		tableName: input.TableName,
		f:         ccfif.f,
	}, nil
}

type changeConsumerFuncInstance struct {
	tableName string
	f         ChangeConsumerFunc
}

func (ccfi *changeConsumerFuncInstance) End() {} // TODO: Snapshot here?
func (ccfi *changeConsumerFuncInstance) Consume(change Change) error {
	return ccfi.f(ccfi.tableName, change)
}

type ChangeConsumerFunc func(tableName string, change Change) error

type changeRowQuerier struct {
	keyspaceName string
	tableName    string
	session      *gocql.Session

	pkCondition string
	bindArgs    []interface{}
}

func newChangeRowQuerier(session *gocql.Session, streams []StreamID, keyspaceName, tableName string) *changeRowQuerier {
	var pkCondition string
	if len(streams) == 1 {
		pkCondition = "\"cdc$stream_id\" = ?"
	} else {
		pkCondition = "\"cdc$stream_id\" IN (?" + strings.Repeat(", ?", len(streams)-1) + ")"
	}

	bindArgs := make([]interface{}, len(streams)+2)
	for i, stream := range streams {
		bindArgs[i] = stream
	}

	return &changeRowQuerier{
		keyspaceName: keyspaceName,
		tableName:    tableName,
		session:      session,

		pkCondition: pkCondition,
		bindArgs:    bindArgs,
	}
}

func (crq *changeRowQuerier) queryRange(start gocql.UUID, end gocql.UUID) (*changeRowIterator, error) {
	// We need metadata to check if there are any tuples
	kmeta, err := crq.session.KeyspaceMetadata(crq.keyspaceName)
	if err != nil {
		return nil, err
	}

	tmeta, ok := kmeta.Tables[crq.tableName+cdcTableSuffix]
	if !ok {
		return nil, fmt.Errorf("no such table: %s.%s", crq.keyspaceName, crq.tableName)
	}

	var colNames []string
	var tupleNames []string
	for _, col := range tmeta.Columns {
		if strings.HasPrefix(col.Type, "frozen<tuple<") || strings.HasPrefix(col.Type, "tuple<") {
			tupleNames = append(tupleNames, col.Name)
			colNames = append(colNames, fmt.Sprintf("writetime(%s)", escapeColumnNameIfNeeded(col.Name)))
		}
	}

	if len(tupleNames) == 0 {
		colNames = []string{"*"}
	} else {
		for name := range tmeta.Columns {
			colNames = append(colNames, escapeColumnNameIfNeeded(name))
		}
	}

	queryStr := fmt.Sprintf(
		"SELECT %s FROM %s.%s%s WHERE %s AND \"cdc$time\" > ? AND \"cdc$time\" < ? BYPASS CACHE",
		strings.Join(colNames, ", "),
		crq.keyspaceName,
		crq.tableName,
		cdcTableSuffix,
		crq.pkCondition,
	)

	crq.bindArgs[len(crq.bindArgs)-2] = start
	crq.bindArgs[len(crq.bindArgs)-1] = end

	iter := crq.session.Query(queryStr, crq.bindArgs...).Iter()
	return newChangeRowIterator(iter, tupleNames)
}

// An adapter over gocql.Iterator
type changeRowIterator struct {
	iter         *gocql.Iter
	columnValues []interface{}

	cdcStreamCols cdcStreamCols
	cdcChangeCols cdcChangeCols

	colInfos        []gocql.ColumnInfo
	tupleNameToIdx  map[string]int
	tupleWriteTimes []int64
}

func newChangeRowIterator(iter *gocql.Iter, tupleNames []string) (*changeRowIterator, error) {
	// TODO: Check how costly is the reflection here
	// We could amortize the cost by preparing the dataFields only at the
	// beginning of the iteration, and change them only if the fields
	// have changed
	// This possibility should be looked into

	allCols := iter.Columns()

	if len(allCols) == 0 {
		// No columns indicate an error
		return nil, iter.Close()
	}

	tupleNameToIdx := make(map[string]int, len(tupleNames))
	for i, name := range tupleNames {
		tupleNameToIdx[name] = i
	}

	ci := &changeRowIterator{
		iter:         iter,
		columnValues: make([]interface{}, 0, len(allCols)),
		colInfos:     make([]gocql.ColumnInfo, 0, len(allCols)),

		tupleNameToIdx:  tupleNameToIdx,
		tupleWriteTimes: make([]int64, len(tupleNames)),
	}

	for i := range tupleNames {
		ci.columnValues = append(ci.columnValues, &ci.tupleWriteTimes[i])
	}

	colNames := make([]string, 0)
	for _, col := range allCols {
		colNames = append(colNames, col.Name)
	}

	for _, col := range allCols[len(tupleNames):] {
		if !strings.HasPrefix(col.Name, "cdc$") {
			ci.colInfos = append(ci.colInfos, col)
		}

		if tupTyp, ok := col.TypeInfo.(gocql.TupleTypeInfo); ok {
			for _, el := range tupTyp.Elems {
				ci.columnValues = append(ci.columnValues, reflect.New(reflect.TypeOf(el.New())).Interface())
			}
		} else {
			var cval interface{}
			switch col.Name {
			case "cdc$stream_id":
				cval = &ci.cdcStreamCols.streamID
			case "cdc$time":
				cval = &ci.cdcStreamCols.time
			case "cdc$batch_seq_no":
				cval = &ci.cdcChangeCols.batchSeqNo
			case "cdc$ttl":
				cval = &ci.cdcChangeCols.ttl
			case "cdc$operation":
				cval = &ci.cdcChangeCols.operation
			case "cdc$end_of_batch":
				cval = &ci.cdcChangeCols.endOfBatch

			default:
				if !strings.HasPrefix(col.Name, "cdc$deleted_") {
					if col.TypeInfo.Type() == gocql.TypeUDT {
						cval = new(udtWithNulls)
					} else {
						// All non-cdc fields should be nullable
						cval = reflect.New(reflect.TypeOf(col.TypeInfo.New())).Interface()
					}
				} else {
					cval = col.TypeInfo.New()
				}
			}
			ci.columnValues = append(ci.columnValues, cval)
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
		data:     make(map[string]interface{}, len(ci.columnValues)-6),
		cdcCols:  ci.cdcChangeCols,
		colInfos: ci.colInfos,
	}

	// At the beginning, there are writetime()
	pos := len(ci.tupleWriteTimes)
	for _, col := range ci.iter.Columns()[len(ci.tupleWriteTimes):] {
		// TODO: Optimize
		if strings.HasPrefix(col.Name, "cdc$") && !strings.HasPrefix(col.Name, "cdc$deleted_") {
			pos++
			continue
		}

		if tupTyp, ok := col.TypeInfo.(gocql.TupleTypeInfo); ok {
			// We deviate from gocql's convention here - we represent a tuple
			// as an []interface{}, we don't keep a separate column for each
			// tuple element.
			// This was made in order to avoid confusion with respect to
			// the cdc log table - if we split tuple v into v[0], v[1], ...,
			// we would also have to artificially split cdc$deleted_v
			// into cdc$deleted_v[0], cdc$deleted_v[1]...

			// Check the writetime of the tuple
			// If the tuple was null, then the writetime will be null (zero in our case)
			// This is a workaround needed because gocql does not differentiate
			// null tuples from tuples which have all their elements as null
			tupLen := len(tupTyp.Elems)
			tupIdx := ci.tupleNameToIdx[col.Name]
			if ci.tupleWriteTimes[tupIdx] != 0 {
				v := make([]interface{}, tupLen)
				for i := 0; i < tupLen; i++ {
					vv := reflect.Indirect(reflect.ValueOf(ci.columnValues[pos+i])).Interface()
					v[i] = adjustBytes(vv)
				}
				change.data[col.Name] = v
			}
			pos += tupLen
		} else if col.TypeInfo.Type() == gocql.TypeUDT {
			v := ci.columnValues[pos].(*udtWithNulls)
			if v != nil {
				change.data[col.Name] = v.fields
				v.fields = nil
			}
			pos++
		} else {
			v, notNull := maybeDereferenceTwice(ci.columnValues[pos])
			if notNull {
				change.data[col.Name] = adjustBytes(v)
			}
			pos++
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

// A wrapper over map[string]interface{} which is used to deserialize UDTs.
// Unlike raw map[string]interface{}, it keeps UDT fields as pointers,
// not values, which allows to determine which values in the UDT are null.
// Remember to pass an initialized map, nil map value won't be good
type udtWithNulls struct {
	fields map[string]interface{}
}

func (uwn *udtWithNulls) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	ptr := reflect.New(reflect.TypeOf(info.New())).Interface()
	if err := gocql.Unmarshal(info, data, ptr); err != nil {
		return err
	}
	if uwn.fields == nil {
		uwn.fields = make(map[string]interface{})
	}
	vv := reflect.Indirect(reflect.ValueOf(ptr)).Interface()
	uwn.fields[name] = adjustBytes(vv)
	return nil
}

func adjustBytes(v interface{}) interface{} {
	// Not sure why, but empty slices get deserialized as []byte(nil).
	// We need to convert it to []byte{} (non-nil, empty slice).
	// This is important because when used in a query,
	// a nil empty slice is treated as null, whereas non-nil
	// slice is treated as an empty slice, which are distinct
	// in CQL.

	switch vTyped := v.(type) {
	case []byte:
		if len(vTyped) == 0 {
			v = make([]byte, 0)
		}
	case *[]byte:
		if vTyped != nil && len(*vTyped) == 0 {
			vv := make([]byte, 0)
			v = &vv
		}
	}
	return v
}

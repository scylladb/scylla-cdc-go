package scyllacdc

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

// OperationType corresponds to the cdc$operation column in CDC log, and
// describes the type of the operation given row represents.
//
// For a comprehensive explanation of what each operation type means,
// see Scylla documentation about CDC.
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

// String is needed to implement the fmt.Stringer interface.
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

// Change represents a group of rows from CDC log with the same cdc$stream_id
// and cdc$time timestamp.
type Change struct {
	// Corresponds to cdc$stream_id.
	StreamID StreamID

	// Corresponds to cdc$time.
	Time gocql.UUID

	// PreImage rows of the group.
	PreImage []*ChangeRow

	// Delta rows of the group.
	Delta []*ChangeRow

	// PostImage rows of the group.
	PostImage []*ChangeRow
}

// GetCassandraTimestamp returns a timestamp of the operation
// suitable to put as a TIMESTAMP parameter to a DML statement
// (INSERT, UPDATE, DELETE).
func (c *Change) GetCassandraTimestamp() int64 {
	return timeuuidToTimestamp(c.Time)
}

// ChangeRow corresponds to a single row from the CDC log.
//
// The ChangeRow uses a slightly different representation of values than gocql's
// MapScan in order to faithfully represent nullability of all values:
//
// Scalar types such as int, text etc. are represented by a pointer to
// their counterpart in gocql (in this case, *int and *string). The only
// exception is the blob, which is encoded as []byte slice - if the column
// was nil, then it will contain a nil slice, if the column was not nil but
// just empty, then the resulting slice will be empty, but not nil.
//
// Tuple types are always represented as an []interface{} slice of values
// in this representation (e.g. tuple<int, text> will contain an *int and
// a *string). If the tuple itself was null, then it will be represented
// as a nil []interface{} slice.
//
// Lists and sets are represented as slices of the corresponding type.
// Because lists and sets cannot contain nils, if a value was to be
// represented as a pointer, it will be represented as a value instead.
// For example, list<int> becomes []int, but list<frozen<tuple<int, text>>
// becomes [][]interface{} because the tuple type cannot be flattened.
//
// Maps are represented as map[K]V, where K and V are in the "flattened" form
// as lists and sets.
//
// UDTs are represented as map[string]interface{}, with values fields being
// represented as described here. For example, a UDT with fields (a int,
// b text) will be represented as a map with two values of types (*int)
// and (*string).
//
// For a comprehensive guide on how to interpret data in the CDC log,
// see Scylla documentation about CDC.
type ChangeRow struct {
	fieldNameToIdx map[string]int

	data     []interface{}
	colInfos []gocql.ColumnInfo

	cdcCols cdcChangeRowCols
}

// Contains columns specific to a change row batch (rows which have
// the same cdc$stream_id and cdc$time)
type cdcChangeBatchCols struct {
	streamID []byte
	time     gocql.UUID
}

// Contains columns specific to a change row, but independent from
// the base table schema.
type cdcChangeRowCols struct {
	batchSeqNo int32
	operation  int8
	ttl        int64
	endOfBatch bool
}

// AtomicChange represents a change to a column of an atomic or a frozen type.
type AtomicChange struct {
	// Value contains the scalar value of the column.
	// If the column was not changed or was deleted, it will be nil.
	//
	// Type: T.
	Value interface{}

	// IsDeleted tells if this column was set to NULL by this change.
	IsDeleted bool
}

// ListChange represents a change to a column of a type list<T>.
type ListChange struct {
	// AppendedElements contains values appended to the list in the form
	// of map from cell timestamps to values.
	//
	// For more information about how to interpret it, see "Advanced column"
	// types" in the CDC documentation.
	//
	// Type: map[gocql.UUID]T
	AppendedElements interface{}

	// RemovedElements contains indices of the removed elements.
	//
	// For more information about how to interpret it, see "Advanced column"
	// types" in the CDC documentation.
	//
	// Type: []gocql.UUID
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
	// type.
	//
	// Type: []T
	AddedElements interface{}

	// RemovedElements contains a slice of values which were removed from the set
	// by the operation. Like AddedValues, it's either a slice or a nil
	// interface.
	//
	// Please note that if the operation overwrote the old value of the set
	// instead of adding/removing elements, this field _will be nil_.
	// Instead, IsReset field will be set, and AddedValues will contain
	// the new state of the set.
	//
	// Type: []T
	RemovedElements interface{}

	// IsReset tells if the set value was overwritten instead of being
	// appended to or removed from. If it's true, than AddedElements will
	// contain the new state of the set (which can be NULL).
	IsReset bool
}

// MapChange represents a change to a column of type map<K, V>.
type MapChange struct {
	// AddedElements contains a map of elements which were added to the map
	// by the operation.
	//
	// Type: map[K]V.
	AddedElements interface{}

	// RemovedElements contains a slice of keys which were removed from the map
	// by the operation.
	// Please note that if the operation overwrote the old value of the map
	// instead of adding/removing elements, this field _will be nil_.
	// Instead, IsReset field will be set, and AddedValues will contain
	// the new state of the map.
	//
	// Type: []K
	RemovedElements interface{}

	// IsReset tells if the map value was overwritten instead of being
	// appended to or removed from. If it's true, than AddedElements will
	// contain the new state of the map (which can be NULL).
	IsReset bool
}

// UDTChange represents a change to a column of a UDT type.
type UDTChange struct {
	// AddedFields contains a map of fields. Non-null value of a field
	// indicate that the field was written to, otherwise it was not written.
	AddedFields map[string]interface{}

	// RemovedFields contains names of fields which were set to null
	// by this operation.
	RemovedFields []string

	// RemovedFieldsIndices contains indices of tields which were set to null
	// by this operation.
	RemovedFieldsIndices []int16

	// IsReset tells if the UDT was overwritten instead of only some fields
	// being overwritten. If this flag is true, then nil fields in AddedFields
	// will mean that those fields should be set to null.
	IsReset bool
}

// GetAtomicChange returns a ScalarChange struct for a given column.
// Results are undefined if the column in the base table was not an atomic type.
func (c *ChangeRow) GetAtomicChange(column string) AtomicChange {
	v, _ := c.GetValue(column)
	isDeleted, _ := c.IsDeleted(column)
	return AtomicChange{
		Value:     v,
		IsDeleted: isDeleted,
	}
}

// GetListChange returns a ListChange struct for a given column.
// Results are undefined if the column in the base table was not a list.
func (c *ChangeRow) GetListChange(column string) ListChange {
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

// GetSetChange returns a SetChange struct for a given column.
// Results are undefined if the column in the base table was not a set.
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

// GetMapChange returns a MapChange struct for a given column.
// Results are undefined if the column in the base table was not a map.
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

// GetUDTChange returns a UDTChange struct for a given column.
// Results are undefined if the column in the base table was not a UDT.
func (c *ChangeRow) GetUDTChange(column string) UDTChange {
	v, _ := c.GetValue(column)
	typedV, _ := v.(map[string]interface{})
	colType, _ := c.GetType(column)
	udtType, _ := colType.(gocql.UDTTypeInfo)
	isDeleted, _ := c.IsDeleted(column)
	deletedElements, _ := c.GetDeletedElements(column)

	typedDeletedElements, _ := deletedElements.([]int16)
	deletedNames := make([]string, 0, len(typedDeletedElements))

	for i, el := range typedDeletedElements {
		if i < len(udtType.Elements) {
			deletedNames = append(deletedNames, udtType.Elements[el].Name)
		}
	}

	udtC := UDTChange{
		AddedFields:          typedV,
		RemovedFieldsIndices: typedDeletedElements,
		RemovedFields:        deletedNames,
		IsReset:              isDeleted,
	}

	return udtC
}

// GetOperation returns the type of operation this change represents.
func (c *ChangeRow) GetOperation() OperationType {
	return OperationType(c.cdcCols.operation)
}

// GetTTL returns TTL for the operation, or 0 if no TTL was used.
func (c *ChangeRow) GetTTL() int64 {
	return c.cdcCols.ttl
}

// GetValue returns value that was assigned to this specific column.
func (c *ChangeRow) GetValue(columnName string) (interface{}, bool) {
	idx, ok := c.fieldNameToIdx[columnName]
	if !ok {
		return nil, false
	}
	return c.data[idx], true
}

// IsDeleted returns a boolean indicating if given column was set to null.
// This only works for clustering columns.
func (c *ChangeRow) IsDeleted(columnName string) (bool, bool) {
	v, ok := c.GetValue("cdc$deleted_" + columnName)
	if !ok {
		return false, false
	}
	vb := v.(*bool)
	return vb != nil && *vb, true
}

// GetDeletedElements returns which elements were deleted from the non-atomic column.
// This function works only for non-atomic columns
func (c *ChangeRow) GetDeletedElements(columnName string) (interface{}, bool) {
	v, ok := c.GetValue("cdc$deleted_elements_" + columnName)
	return v, ok
}

// Columns returns information about data columns in the cdc log table. It contains
// information about all columns - both with and without cdc$ prefix.
func (c *ChangeRow) Columns() []gocql.ColumnInfo {
	return c.colInfos
}

// GetType returns gocql's representation of given column type.
func (c *ChangeRow) GetType(columnName string) (gocql.TypeInfo, bool) {
	idx, ok := c.fieldNameToIdx[columnName]
	if !ok {
		return nil, false
	}
	return c.colInfos[idx].TypeInfo, true
}

// String is needed to implement the fmt.Stringer interface.
func (c *ChangeRow) String() string {
	var b strings.Builder
	b.WriteString(OperationType(c.cdcCols.operation).String())
	b.WriteString(" ")
	b.WriteString(strconv.FormatInt(c.cdcCols.ttl, 10))
	b.WriteString(" -> {")
	first := true

	for _, info := range c.colInfos {
		v, hasValue := c.GetValue(info.Name)
		isDeleted, hasDeleted := c.IsDeleted(info.Name)
		deletedElements, hasDeletedElements := c.GetDeletedElements(info.Name)

		if !first {
			b.WriteString(", ")
		}
		first = false

		b.WriteString(info.Name)
		b.WriteString(":")
		if hasValue {
			b.WriteString(fmt.Sprintf("%v", v))
		} else {
			b.WriteString("nil")
		}

		if hasDeleted {
			b.WriteString(", cdc$deleted_")
			b.WriteString(info.Name)
			b.WriteString(":")
			b.WriteString(fmt.Sprintf("%t", isDeleted))
		}
		if hasDeletedElements {
			b.WriteString(", cdc$deleted_elements_")
			b.WriteString(info.Name)
			b.WriteString(":")
			b.WriteString(fmt.Sprintf("%v", deletedElements))
		}
	}

	b.WriteString("}")
	return b.String()
}

// CreateChangeConsumerInput represents input to the CreateChangeConsumer function.
type CreateChangeConsumerInput struct {
	// Name of the table from which the new ChangeConsumer will receive changes.
	TableName string

	// ID of the stream from which the new ChangeConsumer will receive changes.
	StreamID StreamID

	ProgressReporter *ProgressReporter
}

// ChangeConsumerFactory is used by the library to instantiate ChangeConsumer
// objects when the new generation starts.
type ChangeConsumerFactory interface {
	// Creates a change consumer with given parameters.
	//
	// If this method returns an error, the library will stop with an error.
	CreateChangeConsumer(ctx context.Context, input CreateChangeConsumerInput) (ChangeConsumer, error)
}

// ChangeConsumer processes changes from a single stream of the CDC log.
type ChangeConsumer interface {
	// Processes a change from the CDC log associated with the stream of
	// the ChangeConsumer. This method is called in a sequential manner for each
	// row that appears in the stream.
	//
	// If this method returns an error, the library will stop with an error.
	Consume(ctx context.Context, change Change) error

	// Called after all rows from the stream were consumed, and the reader
	// is about to switch to a new generation, or stop execution altogether.
	//
	// If this method returns an error, the library will stop with an error.
	End() error
}

// MakeChangeConsumerFactoryFromFunc can be used if your processing is very
// simple, and don't need to keep any per-stream state or save any progress.
// The function supplied as an argument will be shared by all consumers created
// by this factory, and will be called for each change in the CDC log.
//
// Please note that the consumers created by this factory do not perform
// any synchronization on their own when calling supplied function, therefore
// you need to guarantee that calling `f` is thread safe.
func MakeChangeConsumerFactoryFromFunc(f ChangeConsumerFunc) ChangeConsumerFactory {
	return &changeConsumerFuncInstanceFactory{f}
}

type changeConsumerFuncInstanceFactory struct {
	f ChangeConsumerFunc
}

// CreateChangeConsumer is needed to implement the ChangeConsumerFactory interface.
func (ccfif *changeConsumerFuncInstanceFactory) CreateChangeConsumer(
	ctx context.Context,
	input CreateChangeConsumerInput,
) (ChangeConsumer, error) {
	return &changeConsumerFuncInstance{
		tableName: input.TableName,
		f:         ccfif.f,
	}, nil
}

type changeConsumerFuncInstance struct {
	tableName string
	f         ChangeConsumerFunc
}

func (ccfi *changeConsumerFuncInstance) End() error {
	return nil
}
func (ccfi *changeConsumerFuncInstance) Consume(ctx context.Context, change Change) error {
	return ccfi.f(ctx, ccfi.tableName, change)
}

// ChangeConsumerFunc can be used in conjunction with MakeChangeConsumerFactoryFromFunc
// if your processing is very simple. For more information, see the description
// of the MakeChangeConsumerFactoryFromFunc function.
type ChangeConsumerFunc func(ctx context.Context, tableName string, change Change) error

type changeRowQuerier struct {
	keyspaceName string
	tableName    string
	session      *gocql.Session

	bindArgs    []interface{}
	consistency gocql.Consistency
}

func newChangeRowQuerier(session *gocql.Session, streams []StreamID, keyspaceName, tableName string, consistency gocql.Consistency) *changeRowQuerier {
	bindArgs := make([]interface{}, 3)
	streamsIface := make([]interface{}, len(streams))
	for i, stream := range streams {
		streamsIface[i] = stream
	}
	bindArgs[0] = streamsIface

	return &changeRowQuerier{
		keyspaceName: keyspaceName,
		tableName:    tableName,
		session:      session,

		bindArgs:    bindArgs,
		consistency: consistency,
	}
}

func (crq *changeRowQuerier) queryRange(start, end gocql.UUID) (*changeRowIterator, error) {
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
		var ct interface{} = col.Type
		var ctStr string
		switch ct := ct.(type) {
		case string:
			ctStr = ct
		case fmt.Stringer:
			ctStr = ct.String()
		}
		if strings.HasPrefix(ctStr, "frozen<tuple<") || strings.HasPrefix(ctStr, "tuple<") || strings.HasPrefix(ctStr, "tuple(") {
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
		"SELECT %s FROM %s.%s%s WHERE \"cdc$stream_id\" IN ? AND \"cdc$time\" > ? AND \"cdc$time\" <= ? BYPASS CACHE",
		strings.Join(colNames, ", "),
		crq.keyspaceName,
		crq.tableName,
		cdcTableSuffix,
	)

	crq.bindArgs[len(crq.bindArgs)-2] = start
	crq.bindArgs[len(crq.bindArgs)-1] = end

	iter := crq.session.Query(queryStr, crq.bindArgs...).Consistency(crq.consistency).Iter()
	return newChangeRowIterator(iter, tupleNames)
}

// For a given range, returns the cdc$time of the earliest rows for each stream.
// func (crq *changeRowQuerier) findFirstRowsInRange(start, end gocql.UUID) (map[string]gocql.UUID, error) {
// 	queryStr := fmt.Sprintf(
// 		"SELECT \"cdc$stream_id\", \"cdc$time\" FROM %s.%s%s WHERE %s AND \"cdc$time\" > ? AND \"cdc$time\" <= ? PER PARTITION LIMIT 1 BYPASS CACHE",
// 		crq.keyspaceName,
// 		crq.tableName,
// 		cdcTableSuffix,
// 		crq.pkCondition,
// 	)

// 	crq.bindArgs[len(crq.bindArgs)-2] = start
// 	crq.bindArgs[len(crq.bindArgs)-1] = end

// 	ret := make(map[string]gocql.UUID)
// 	iter := crq.session.Query(queryStr, crq.bindArgs...).Consistency(crq.consistency).Iter()

// 	var (
// 		streamID StreamID
// 		cdcTime  gocql.UUID
// 	)

// 	for iter.Scan(&streamID, &cdcTime) {
// 		ret[string(streamID)] = cdcTime
// 	}

// 	if err := iter.Close(); err != nil {
// 		return nil, err
// 	}
// 	return ret, nil
// }

// An adapter over gocql.Iterator which chooses representation for row values
// which is more suitable for CDC than the default one.
//
// Gocql has two main methods of retrieving row data:
//
// - If you know what columns will be returned by the query and which types
//   to use to represent them, you use (*Iter).Scan(...) function and pass
//   a list of pointers to values of types you chose for the representation.
//   For example, if `x` is int, `Scan(&x)` will put the value of the column
//   directly to the `x` variable, setting it to 0 if the column was null.
// - If you don't know which columns will be returned and what are their
//   types, you can use (*Iter).MapScan, which returns a map from column
//   name to the column value. Gocql automatically chooses a type which
//   will be used to represent the column value.
//
// In our interface, we would like to use an API like MapScan, but there
// are some problems which are addressed by changeRowIterator:
//
// - Gocql's choice of the type used to represent column values is not the best
//   for CDC use case. First and foremost, it's very important to differentiate
//   Go's default value for a type from a null. For example, for int columns,
//   MapScan chooses Go's int type, and sets it to 0 in both cases if it was 0
//   or null in the table. For CDC, this means completely different things -
//   0 would mean that the 0 value was written to that column, while null would
//   mean that this column value was not changed.
//   Fortunately, we can solve this issue by using a pointer-to-type (e.g. *int).
//   Gocql will set it to null if it was null in the database, and set it
//   to a pointer to a proper value if it was not null.
//
// - Similarly to above, UDTs suffer from a similar problem - they are,
//   by default, represented by a map[string]interface{} which holds non-pointer
//   values of UDT's elements. Fortunately, we can provide a custom type
//   which uses pointers to UDT's elements - see udtWithNulls.
//
// - Tuples are handled in a peculiar way - instead of returning, for example,
//   an []interface{} which holds tuple values, Scan expects that a pointer
//   for each tuple element will be provided, and MapScan puts each tuple
//   element under a separate key in the map. This creates a problem - it's
//   impossible to differentiate a tuple with all fields set to null, and
//   a tuple that is just a null. In CDC, the first means an overwrite of the
//   column, and the second means that the column should not be changed.
//   This is worked around by using the writetime(X) function on the tuple
//   column - this function returns null iff column X was null.
//   Moreover, tuples are represented as an []interface{} slice containing
//   pointers to tuple elements.
type changeRowIterator struct {
	iter         *gocql.Iter
	columnValues []interface{}

	cdcChangeBatchCols cdcChangeBatchCols
	cdcChangeRowCols   cdcChangeRowCols

	// Contains information on all columns apart from the writetime() ones
	colInfos []gocql.ColumnInfo

	// Maps from tuple column names to the index they occupy in columnValues
	tupleNameToWritetimeIdx map[string]int
	// Maps from column name to index in change slice
	fieldNameToIdx map[string]int

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

	// If there are tuples in the table, the query will have form
	//   SELECT writetime(X), writetime(Z), X, Y, Z FROM ...
	// where X and Z are tuples.
	// We need to get the writetime for tuples in order to work around
	// an issue in gocql - otherwise we wouldn't be able to differentiate
	// a tuple with all columns null, and a tuple which is null itself.

	// Assign slots in the beginning for tuples' writetime
	tupleNameToWritetimeIdx := make(map[string]int, len(tupleNames))
	for i, name := range tupleNames {
		tupleNameToWritetimeIdx[name] = i
	}

	ci := &changeRowIterator{
		iter:         iter,
		columnValues: make([]interface{}, 0, len(allCols)),
		colInfos:     make([]gocql.ColumnInfo, 0, len(allCols)),

		tupleNameToWritetimeIdx: tupleNameToWritetimeIdx,
		fieldNameToIdx:          make(map[string]int),
		tupleWriteTimes:         make([]int64, len(tupleNames)),
	}

	// tupleWriteTimes will receive results of the writetime function
	// for each tuple column
	for i := range tupleNames {
		ci.columnValues = append(ci.columnValues, &ci.tupleWriteTimes[i])
	}

	ci.colInfos = allCols[len(tupleNames):]

	for colIdx, col := range ci.colInfos {
		if tupTyp, ok := col.TypeInfo.(gocql.TupleTypeInfo); ok {
			// Gocql operates on "flattened" tuples, therefore we need to put
			// a separate value for each tuple element.
			// To represent a field, use value returned by gocql's TypeInfo.New(),
			// but convert it into a pointer
			ci.fieldNameToIdx[col.Name] = colIdx
			for range tupTyp.Elems {
				ci.columnValues = append(ci.columnValues, &withNullUnmarshaler{})
			}
		} else {
			var cval interface{}

			// For common cdc column names, we want their values to be placed
			// in cdcChangeBatchCols and cdcChangeRowCols structures
			switch col.Name {
			case "cdc$stream_id":
				cval = &ci.cdcChangeBatchCols.streamID
			case "cdc$time":
				cval = &ci.cdcChangeBatchCols.time
			case "cdc$batch_seq_no":
				cval = &ci.cdcChangeRowCols.batchSeqNo
			case "cdc$ttl":
				cval = &ci.cdcChangeRowCols.ttl
			case "cdc$operation":
				cval = &ci.cdcChangeRowCols.operation
			case "cdc$end_of_batch":
				cval = &ci.cdcChangeRowCols.endOfBatch

			default:
				cval = &withNullUnmarshaler{}
			}
			ci.fieldNameToIdx[col.Name] = colIdx
			ci.columnValues = append(ci.columnValues, cval)
		}
	}

	return ci, nil
}

func (ci *changeRowIterator) Next() (cdcChangeBatchCols, *ChangeRow) {
	if !ci.iter.Scan(ci.columnValues...) {
		return cdcChangeBatchCols{}, nil
	}

	change := &ChangeRow{
		fieldNameToIdx: ci.fieldNameToIdx,

		data:     make([]interface{}, len(ci.colInfos)),
		colInfos: ci.colInfos,

		cdcCols: ci.cdcChangeRowCols,
	}

	// Beginning of tupleWriteTimes contains
	// At the beginning, there are writetime() for tuples. Skip them
	pos := len(ci.tupleWriteTimes)
	for idxInSlice, col := range ci.colInfos {
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
			tupIdx := ci.tupleNameToWritetimeIdx[col.Name]
			if ci.tupleWriteTimes[tupIdx] != 0 {
				v := make([]interface{}, tupLen)
				for i := 0; i < tupLen; i++ {
					vv := ci.columnValues[pos+i].(*withNullUnmarshaler).value
					v[i] = adjustBytes(vv)
				}
				change.data[idxInSlice] = v
			} else {
				change.data[idxInSlice] = ([]interface{})(nil)
			}
			pos += tupLen
		} else {
			v, isWithNull := ci.columnValues[pos].(*withNullUnmarshaler)
			if isWithNull {
				change.data[idxInSlice] = v.value
			} else {
				change.data[idxInSlice] = dereference(ci.columnValues[pos])
			}
			pos++
		}
	}
	return ci.cdcChangeBatchCols, change
}

func (ci *changeRowIterator) Close() error {
	return ci.iter.Close()
}

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}

// Converts a v1 UUID to a Cassandra timestamp.
// UUID timestamp is measured in 100-nanosecond intervals since 00:00:00.00, 15 October 1582.
// Cassandra timestamp is measured in milliseconds since 00:00:00.00, 1 January 1970.
func timeuuidToTimestamp(from gocql.UUID) int64 {
	return (from.Timestamp() - 0x01b21dd213814000) / 10
}

type withNullUnmarshaler struct {
	value interface{}
}

func (wnu *withNullUnmarshaler) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	switch info.Type() {
	case gocql.TypeUDT:
		// UDTs are unmarshaled as map[string]interface{}
		// Returned map is nil iff the whole UDT value was nil
		if data == nil {
			wnu.value = (map[string]interface{})(nil)
			return nil
		}
		udtInfo := info.(gocql.UDTTypeInfo)
		uwn := udtWithNulls{make(map[string]interface{}, len(udtInfo.Elements))}
		if err := gocql.Unmarshal(info, data, &uwn); err != nil {
			return err
		}
		wnu.value = uwn.fields
		return nil

	case gocql.TypeTuple:
		// Tuples are unmarshaled as []interface{}
		// Returned slice is nil iff the whole tuple is nil
		if data == nil {
			wnu.value = ([]interface{})(nil)
			return nil
		}

		// Make a tuple with withNullMarshallers
		tupInfo := info.(gocql.TupleTypeInfo)
		tupValue := make([]interface{}, len(tupInfo.Elems))
		for i := range tupValue {
			tupValue[i] = &withNullUnmarshaler{}
		}

		if err := gocql.Unmarshal(info, data, tupValue); err != nil {
			return err
		}

		// Unwrap tuple values
		for i := range tupValue {
			tupValue[i] = tupValue[i].(*withNullUnmarshaler).value
		}
		wnu.value = tupValue
		return nil

	case gocql.TypeList, gocql.TypeSet:
		if data == nil {
			wnu.value = reflect.ValueOf(info.New()).Elem().Interface()
			return nil
		}

		// Make a list with withNullMarshallers
		var lWnm []withNullUnmarshaler
		if err := gocql.Unmarshal(info, data, &lWnm); err != nil {
			return err
		}

		lV := reflect.ValueOf(info.New()).Elem()
		for _, wnm := range lWnm {
			lV.Set(reflect.Append(lV, reflect.ValueOf(wnm.derefForListOrMap())))
		}
		wnu.value = lV.Interface()
		return nil

	case gocql.TypeMap:
		if data == nil {
			wnu.value = reflect.ValueOf(info.New()).Elem().Interface()
			return nil
		}

		// Make a map with withNullMarshallers
		mapInfo := info.(gocql.CollectionType)
		keyType := reflect.TypeOf(mapInfo.Key.New()).Elem()
		mapWithWnuType := reflect.MapOf(keyType, reflect.TypeOf(withNullUnmarshaler{}))
		mapWithWnuPtr := reflect.New(mapWithWnuType)
		mapWithWnuPtr.Elem().Set(reflect.MakeMap(mapWithWnuType))
		if err := gocql.Unmarshal(info, data, mapWithWnuPtr.Interface()); err != nil {
			return err
		}

		resultMapType := reflect.TypeOf(info.New()).Elem()
		resultMap := reflect.MakeMap(resultMapType)
		iter := mapWithWnuPtr.Elem().MapRange()
		for iter.Next() {
			unwrapped := iter.Value().Interface().(withNullUnmarshaler)
			resultMap.SetMapIndex(iter.Key(), reflect.ValueOf(unwrapped.derefForListOrMap()))
		}
		wnu.value = resultMap.Interface()
		return nil

	case gocql.TypeBlob:
		if data == nil {
			wnu.value = ([]byte)(nil)
			return nil
		}

		slice := make([]byte, 0)
		if err := gocql.Unmarshal(info, data, &slice); err != nil {
			return err
		}
		wnu.value = slice
		return nil

	default:
		vptr := reflect.New(reflect.TypeOf(info.New()))
		if err := gocql.Unmarshal(info, data, vptr.Interface()); err != nil {
			return err
		}

		wnu.value = vptr.Elem().Interface()
		return nil
	}
}

func (wnu *withNullUnmarshaler) derefForListOrMap() interface{} {
	v := reflect.ValueOf(wnu.value)
	if v.Kind() == reflect.Ptr {
		return v.Elem().Interface()
	}
	return wnu.value
}

// A wrapper over map[string]interface{} which is used to deserialize UDTs.
// Unlike raw map[string]interface{}, it keeps UDT fields as pointers,
// not values, which allows to determine which values in the UDT are null.
// Remember to pass an initialized map, nil map value won't be good
type udtWithNulls struct {
	fields map[string]interface{}
}

func (uwn *udtWithNulls) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	var wnu withNullUnmarshaler
	if err := gocql.Unmarshal(info, data, &wnu); err != nil {
		return err
	}
	if uwn.fields == nil {
		uwn.fields = make(map[string]interface{})
	}
	uwn.fields[name] = wnu.value
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

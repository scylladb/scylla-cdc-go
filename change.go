package scylla_cdc

import "github.com/gocql/gocql"

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

// Change corresponds to a single row from the cdc log
type Change struct {
	data map[string]interface{}
}

// GetOperation returns the type of operation this change represents
func (c *Change) GetOperation() OperationType {
	return OperationType(c.data["cdc$operation"].(int8))
}

// GetTime returns the timestamp of the write
func (c *Change) GetTime() gocql.UUID {
	return c.data["cdc$time"].(gocql.UUID)
}

// GetTTL returns 0 if TTL was not set for this operation
func (c *Change) GetTTL() int64 {
	return c.data["cdc$ttl"].(int64)
}

// GetValue returns value that was assigned to this specific column
func (c *Change) GetValue(columnName string) interface{} {
	return c.data[columnName]
}

// IsDeleted returns a boolean indicating if given column was set to null.
// This only works for clustering columns.
func (c *Change) IsDeleted(columnName string) bool {
	return c.data["cdc$deleted_"+columnName].(bool)
}

// GetDeletedElements returns which elements were deleted from the non-atomic column.
// This function works only for non-atomic columns
func (c *Change) GetDeletedElements(columnName string) interface{} {
	return c.data["cdc$deleted_elements_"+columnName]
}

type ChangeConsumer interface {
	Consume(Change)
}

type ChangeConsumerFunc func(Change)

func (ccf ChangeConsumerFunc) Consume(c Change) {
	ccf(c)
}

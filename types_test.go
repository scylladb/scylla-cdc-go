package scylla_cdc

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

const (
	// TODO: Take it from env
	address = "127.0.0.1"
)

type change map[string]interface{}

var typesTestCases = []struct {
	tableName       string
	schema          string
	stmts           []string
	expectedChanges []change
}{
	{
		"ks.types_simple",
		"CREATE TABLE %s (pk int, ck int, v1 int, v2 text, v3 blob, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v1) VALUES (1, 2, 3)",
			"UPDATE %s SET v1 = 7 WHERE pk = 1 AND ck = 2",
			"DELETE v1 FROM %s WHERE pk = 1 AND ck = 2",

			"INSERT INTO %s (pk, ck, v2) VALUES (1, 3, 'abc')",
			"UPDATE %s SET v2 = 'def' WHERE pk = 1 AND ck = 3",
			"UPDATE %s SET v2 = '' WHERE pk = 1 AND ck = 3",
			"DELETE v2 FROM %s WHERE pk = 1 AND ck = 3",

			"INSERT INTO %s (pk, ck, v3) VALUES (1, 4, 0x1234)",
			"UPDATE %s SET v3 = 0x4321 WHERE pk = 1 AND ck = 4",
			"UPDATE %s SET v3 = 0x WHERE pk = 1 AND ck = 4",
			"DELETE v3 FROM %s WHERE pk = 1 AND ck = 4",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v1": scalarOverwrite{ptrTo(int(3))}},
			{"cdc$operation": OperationType(Update), "v1": scalarOverwrite{ptrTo(int(7))}},
			{"cdc$operation": OperationType(Update), "v1": scalarErase{}},

			{"cdc$operation": OperationType(Insert), "v2": scalarOverwrite{ptrTo("abc")}},
			{"cdc$operation": OperationType(Update), "v2": scalarOverwrite{ptrTo("def")}},
			{"cdc$operation": OperationType(Update), "v2": scalarOverwrite{ptrTo("")}},
			{"cdc$operation": OperationType(Update), "v2": scalarErase{}},

			{"cdc$operation": OperationType(Insert), "v3": scalarOverwrite{[]byte{0x12, 0x34}}},
			{"cdc$operation": OperationType(Update), "v3": scalarOverwrite{[]byte{0x43, 0x21}}},
			{"cdc$operation": OperationType(Update), "v3": scalarOverwrite{make([]byte, 0)}},
			{"cdc$operation": OperationType(Update), "v3": scalarErase{}},
		},
	},
	{
		"ks.types_lists",
		"CREATE TABLE %s (pk int, ck int, v list<int>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, [1, 2, 3])",
			"UPDATE %s SET v = [4, 5, 6] WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v + [7, 8, 9] WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = [-2, -1, 0] + v WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v - [5, 6, 7, 8] WHERE pk = 1 AND ck = 1",
			"DELETE v FROM %s WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": listOverwrite{[]int{1, 2, 3}}},
			{"cdc$operation": OperationType(Update), "v": listOverwrite{[]int{4, 5, 6}}},
			{"cdc$operation": OperationType(Update), "v": listAddition{[]int{7, 8, 9}}},
			{"cdc$operation": OperationType(Update), "v": listAddition{[]int{-2, -1, 0}}},
			{"cdc$operation": OperationType(Update), "v": listRemoval{4}},
			{"cdc$operation": OperationType(Update), "v": collectionErase{}},
		},
	}, {
		"ks.types_lists_with_tuples",
		"CREATE TABLE %s (pk int, ck int, v list<frozen<tuple<int, text>>>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, [(1, 'abc'), (2, 'def')])",
			"UPDATE %s SET v = [(null, 'ghi'), (4, null)] WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v + [(5, 'mno')] WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = [(6, 'pqr')] + v WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v - [(5, 'mno'), (6, 'pqr')] WHERE pk = 1 AND ck = 1",
			"DELETE v FROM %s WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": listOverwrite{[][]interface{}{{ptrTo(1), ptrTo("abc")}, {ptrTo(2), ptrTo("def")}}}},
			{"cdc$operation": OperationType(Update), "v": listOverwrite{[][]interface{}{{(*int)(nil), ptrTo("ghi")}, {ptrTo(4), (*string)(nil)}}}},
			{"cdc$operation": OperationType(Update), "v": listAddition{[][]interface{}{{ptrTo(5), ptrTo("mno")}}}},
			{"cdc$operation": OperationType(Update), "v": listAddition{[][]interface{}{{ptrTo(6), ptrTo("pqr")}}}},
			{"cdc$operation": OperationType(Update), "v": listRemoval{2}},
			{"cdc$operation": OperationType(Update), "v": collectionErase{}},
		},
	},
	{
		"ks.types_maps",
		"CREATE TABLE %s (pk int, ck int, v map<int, text>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, {1: 'abc', 2: 'def'})",
			"UPDATE %s SET v = {3: 'ghi', 4: 'jkl'} WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v + {5: 'mno', 6: 'pqr'} WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v - {4, 5} WHERE pk = 1 AND ck = 1",
			"DELETE v FROM %s WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[int]string{1: "abc", 2: "def"}}},
			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{map[int]string{3: "ghi", 4: "jkl"}}},
			{"cdc$operation": OperationType(Update), "v": collectionAddition{map[int]string{5: "mno", 6: "pqr"}}},
			{"cdc$operation": OperationType(Update), "v": collectionRemoval{[]int{4, 5}}},
			{"cdc$operation": OperationType(Update), "v": collectionErase{}},
		},
	},
	{
		"ks.types_sets",
		"CREATE TABLE %s (pk int, ck int, v set<int>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, {1, 2})",
			"UPDATE %s SET v = {3, 4} WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v + {5, 6} WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v - {4, 5} WHERE pk = 1 AND ck = 1",
			"DELETE v FROM %s WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{[]int{1, 2}}},
			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{[]int{3, 4}}},
			{"cdc$operation": OperationType(Update), "v": collectionAddition{[]int{5, 6}}},
			{"cdc$operation": OperationType(Update), "v": collectionRemoval{[]int{4, 5}}},
			{"cdc$operation": OperationType(Update), "v": collectionErase{}},
		},
	},
	{
		"ks.types_sets_of_udts",
		"CREATE TABLE %s (pk int, ck int, v set<frozen<ks.udt>>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, {(1, 'abc'), (2, 'def')})",
			"UPDATE %s SET v = {(null, 'ghi'), (4, null)} WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v + {(5, 'mno')} WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = v - {(5, 'mno')} WHERE pk = 1 AND ck = 1",
			"DELETE v FROM %s WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{[]map[string]interface{}{{"a": ptrTo(1), "b": ptrTo("abc")}, {"a": ptrTo(2), "b": ptrTo("def")}}}},
			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{[]map[string]interface{}{{"a": (*int)(nil), "b": ptrTo("ghi")}, {"a": ptrTo(4), "b": (*string)(nil)}}}},
			{"cdc$operation": OperationType(Update), "v": collectionAddition{[]map[string]interface{}{{"a": ptrTo(5), "b": ptrTo("mno")}}}},
			{"cdc$operation": OperationType(Update), "v": collectionRemoval{[]map[string]interface{}{{"a": ptrTo(5), "b": ptrTo("mno")}}}},
			{"cdc$operation": OperationType(Update), "v": collectionErase{}},
		},
	},
	{
		"ks.types_tuples",
		"CREATE TABLE %s (pk int, ck int, v tuple<int, text>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (2, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (2, null))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (null, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (null, null))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, null)",

			"UPDATE %s SET v = (2, 'abc') WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (2, null) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (null, 'abc') WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (null, '') WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (null, null) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = null WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": scalarOverwrite{[]interface{}{ptrTo(2), ptrTo("abc")}}},
			{"cdc$operation": OperationType(Insert), "v": scalarOverwrite{[]interface{}{ptrTo(2), (*string)(nil)}}},
			{"cdc$operation": OperationType(Insert), "v": scalarOverwrite{[]interface{}{(*int)(nil), ptrTo("abc")}}},
			{"cdc$operation": OperationType(Insert), "v": scalarOverwrite{[]interface{}{(*int)(nil), (*string)(nil)}}},
			{"cdc$operation": OperationType(Insert), "v": scalarErase{}},

			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{ptrTo(2), ptrTo("abc")}}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{ptrTo(2), (*string)(nil)}}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{(*int)(nil), ptrTo("abc")}}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{(*int)(nil), ptrTo("")}}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{(*int)(nil), (*string)(nil)}}},
			{"cdc$operation": OperationType(Update), "v": scalarErase{}},
		},
	},
	{
		"ks.types_tuples_in_tuples",
		"CREATE TABLE %s (pk int, ck int, v tuple<tuple<int, text>, int>, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, ((1, 'abc'), 7))",

			"UPDATE %s SET v = ((100, 'zyx'), 111) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = null WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = ((200, null), 999) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = ((300, ''), 333) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (null, 444) WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": scalarOverwrite{[]interface{}{[]interface{}{ptrTo(1), ptrTo("abc")}, ptrTo(7)}}},

			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{[]interface{}{ptrTo(100), ptrTo("zyx")}, ptrTo(111)}}},
			{"cdc$operation": OperationType(Update), "v": scalarErase{}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{[]interface{}{ptrTo(200), (*string)(nil)}, ptrTo(999)}}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{[]interface{}{ptrTo(300), ptrTo("")}, ptrTo(333)}}},
			{"cdc$operation": OperationType(Update), "v": scalarOverwrite{[]interface{}{([]interface{})(nil), ptrTo(444)}}},
		},
	},
	{
		"ks.types_udts",
		"CREATE TABLE %s (pk int, ck int, v ks.udt, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (2, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (2, null))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (null, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (null, null))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, null)",

			"UPDATE %s SET v = (2, 'abc') WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (2, null) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (null, 'abc') WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = (null, null) WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v = null WHERE pk = 1 AND ck = 1",

			"UPDATE %s SET v.a = 2 WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v.a = null WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v.b = 'abc' WHERE pk = 1 AND ck = 1",
			"UPDATE %s SET v.b = null WHERE pk = 1 AND ck = 1",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": ptrTo(2), "b": ptrTo("abc")}}},
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": ptrTo(2), "b": (*string)(nil)}}},
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": (*int)(nil), "b": ptrTo("abc")}}},
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": (*int)(nil), "b": (*string)(nil)}}},
			{"cdc$operation": OperationType(Insert), "v": collectionErase{}},

			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{map[string]interface{}{"a": ptrTo(2), "b": ptrTo("abc")}}},
			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{map[string]interface{}{"a": ptrTo(2), "b": (*string)(nil)}}},
			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{map[string]interface{}{"a": (*int)(nil), "b": ptrTo("abc")}}},
			{"cdc$operation": OperationType(Update), "v": collectionOverwrite{map[string]interface{}{"a": (*int)(nil), "b": (*string)(nil)}}},
			{"cdc$operation": OperationType(Update), "v": collectionErase{}},

			{"cdc$operation": OperationType(Update), "v": udtSetFields{map[string]interface{}{"a": ptrTo(2), "b": (*string)(nil)}, []int16(nil)}},
			{"cdc$operation": OperationType(Update), "v": udtSetFields{map[string]interface{}{"a": (*int)(nil), "b": (*string)(nil)}, []int16{0}}},
			{"cdc$operation": OperationType(Update), "v": udtSetFields{map[string]interface{}{"a": (*int)(nil), "b": ptrTo("abc")}, []int16(nil)}},
			{"cdc$operation": OperationType(Update), "v": udtSetFields{map[string]interface{}{"a": (*int)(nil), "b": (*string)(nil)}, []int16{1}}},
		},
	},
	{
		"ks.types_nested_udts",
		"CREATE TABLE %s (pk int, ck int, v ks.udt_nested, PRIMARY KEY (pk, ck))",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, ((2, 'abc'), 3))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, ((null, 'abc'), null))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (null, 3))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, (null, null))",
			"INSERT INTO %s (pk, ck, v) VALUES (1, 1, null)",
		},
		[]change{
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": map[string]interface{}{"a": ptrTo(2), "b": ptrTo("abc")}, "b": ptrTo(3)}}},
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": map[string]interface{}{"a": (*int)(nil), "b": ptrTo("abc")}, "b": (*int)(nil)}}},
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": (map[string]interface{})(nil), "b": ptrTo(3)}}},
			{"cdc$operation": OperationType(Insert), "v": collectionOverwrite{map[string]interface{}{"a": (map[string]interface{})(nil), "b": (*int)(nil)}}},
			{"cdc$operation": OperationType(Insert), "v": collectionErase{}},
		},
	},
}

type scalarOverwrite struct {
	value interface{}
}
type scalarErase struct{}

type collectionOverwrite struct {
	value interface{}
}
type collectionErase struct{}
type collectionAddition struct {
	values interface{}
}
type collectionRemoval struct {
	values interface{}
}

type listOverwrite struct {
	values interface{}
}
type listAddition struct {
	values interface{}
}
type listRemoval struct {
	// With deltas, it's impossible to tell which values were removed,
	// so we may only check the count
	elementCount int
}

type udtSetFields struct {
	set     map[string]interface{}
	removed []int16
}

type notSet struct{}

func TestTypes(t *testing.T) {
	var rowsMu sync.Mutex
	changeRows := make(map[string][]*ChangeRow)

	factory := MakeChangeConsumerFactoryFromFunc(func(ctx context.Context, tableName string, c Change) error {
		rowsMu.Lock()
		defer rowsMu.Unlock()
		changeRows[tableName] = append(changeRows[tableName], c.Delta...)
		return nil
	})

	adv := AdvancedReaderConfig{
		ChangeAgeLimit:         time.Minute,
		PostNonEmptyQueryDelay: 3 * time.Second,
		PostEmptyQueryDelay:    3 * time.Second,
		PostFailedQueryDelay:   3 * time.Second,
		QueryTimeWindowSize:    5 * time.Minute,
		ConfidenceWindowSize:   0,
	}

	// Configure a session
	cluster := gocql.NewCluster(address)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// Create all tables and input all data
	execQuery(t, session, "DROP KEYSPACE IF EXISTS ks")
	execQuery(t, session, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

	execQuery(t, session, "CREATE TYPE ks.udt (a int, b text)")
	execQuery(t, session, "CREATE TYPE ks.udt_nested (a frozen<ks.udt>, b int)")

	for _, tc := range typesTestCases {
		execQuery(t, session, fmt.Sprintf(tc.schema, tc.tableName)+" WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true}")

		for _, stmt := range tc.stmts {
			execQuery(t, session, fmt.Sprintf(stmt, tc.tableName))
		}
	}

	var tableNames []string
	for _, tc := range typesTestCases {
		tableNames = append(tableNames, tc.tableName)
	}

	// Configuration for the CDC reader
	cfg := NewReaderConfig(
		session,
		factory,
		&NoProgressManager{},
		tableNames...,
	)
	cfg.Advanced = adv
	cfg.Logger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	reader, err := NewReader(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Run the replicator and collect all changes
	errC := make(chan error)
	go func() { errC <- reader.Run(context.Background()) }()

	time.Sleep(time.Second)

	reader.StopAt(time.Now().Add(time.Second))

	if err := <-errC; err != nil {
		t.Fatal(err)
	}

	// Verify
	t.Log("verifying results")
	for _, tc := range typesTestCases {
		changesForTable := changeRows[tc.tableName]

		if len(changesForTable) != len(tc.expectedChanges) {
			t.Errorf("%s: expected %d changes, got %d", tc.tableName, len(tc.expectedChanges), len(changesForTable))
		}

		for i, change := range changesForTable {
			expected := tc.expectedChanges[i]
			for columnName, v := range expected {
				if columnName == "cdc$operation" {
					expectedOp := v.(OperationType)
					if expectedOp != change.GetOperation() {
						t.Errorf("%s[%d]: expected operation %s, got %s", tc.tableName, i, expectedOp, change.GetOperation())
					}
					continue
				}

				changeValue, isPresent := change.GetValue(columnName)
				if !isPresent {
					t.Errorf("%s[%d]: expected column %s to be present", tc.tableName, i, columnName)
				}
				if changeValue == nil {
					t.Errorf("%s[%d]: column %s is nil", tc.tableName, i, columnName)
				}

				isDeleted, ok := change.IsDeleted(columnName)
				if !ok {
					t.Errorf("%s[%d]: no cdc$deleted_%s column", tc.tableName, i, columnName)
					continue
				}

				deletedElements, hasDeletedElements := change.GetDeletedElements(columnName)

				switch v := v.(type) {
				case scalarOverwrite:
					if isDeleted {
						t.Errorf("%s[%d]: expected overwrite of %s, but got deletion", tc.tableName, i, columnName)
					}
					if !reflect.DeepEqual(changeValue, v.value) {
						t.Errorf("%s[%d]: expected value of %s to be %#v, but is %#v", tc.tableName, i, columnName, v.value, changeValue)
					}

				case scalarErase:
					if !isDeleted {
						t.Errorf("%s[%d]: expected %s to be deleted", tc.tableName, i, columnName)
					}
					if !reflect.ValueOf(changeValue).IsNil() {
						t.Errorf("%s[%d]: expected %s to be deleted, but got value %#v", tc.tableName, i, columnName, changeValue)
					}

				case collectionOverwrite:
					if !isDeleted {
						t.Errorf("%s[%d]: expected overwrite of %s, but got append", tc.tableName, i, columnName)
					}
					if !reflect.DeepEqual(changeValue, v.value) {
						t.Errorf("%s[%d]: expected value of %s to be %#v, but is %#v", tc.tableName, i, columnName, v.value, changeValue)
					}
					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if deletedElements != nil && reflect.ValueOf(deletedElements).Len() != 0 {
						t.Errorf("%s[%d]: expected %s not to have deleted elements, but has %#v", tc.tableName, i, columnName, deletedElements)
					}

				case collectionErase:
					if !isDeleted {
						t.Errorf("%s[%d]: expected %s to be deleted", tc.tableName, i, columnName)
					}
					// if isPresent {
					// 	t.Errorf("%s[%d]: expected elements of %s to not be present, but it has %#v", tc.tableName, i, columnName, changeValue)
					// }
					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if deletedElements != nil && reflect.ValueOf(deletedElements).Len() != 0 { // TODO: This could be tighter
						t.Errorf("%s[%d]: expected %s not to have deleted elements, but has %#v", tc.tableName, i, columnName, deletedElements)
					}

				case collectionAddition:
					if isDeleted {
						t.Errorf("%s[%d]: expected append of %s, but it is an overwrite", tc.tableName, i, columnName)
					}
					if !reflect.DeepEqual(changeValue, v.values) {
						t.Errorf("%s[%d]: expected value of %s to be %#v, but is %#v", tc.tableName, i, columnName, v.values, changeValue)
					}
					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if deletedElements != nil && reflect.ValueOf(deletedElements).Len() != 0 {
						t.Errorf("%s[%d]: expected %s not to have deleted elements, but has %v", tc.tableName, i, columnName, deletedElements)
					}

				case collectionRemoval:
					if isDeleted {
						t.Errorf("%s[%d]: expected removal from of %s, but it is an overwrite", tc.tableName, i, columnName)
					}
					if !reflect.ValueOf(changeValue).IsNil() {
						t.Errorf("%s[%d]: expected elements of %s to not be present", tc.tableName, i, columnName)
					}
					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if !reflect.DeepEqual(deletedElements, v.values) {
						t.Errorf("%s[%d]: expected %s deleted elements to be %#v, but is %#v", tc.tableName, i, columnName, v.values, deletedElements)
					}

				case listOverwrite:
					if !isDeleted {
						t.Errorf("%s[%d]: expected %s to be deleted", tc.tableName, i, columnName)
					}

					checkList(t, tc.tableName, columnName, i, changeValue, v.values)

					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if !reflect.ValueOf(deletedElements).IsNil() {
						t.Errorf("%s[%d]: expected %s deleted elements to be nil, but is %#v", tc.tableName, i, columnName, deletedElements)
					}

				case listAddition:
					if isDeleted {
						t.Errorf("%s[%d]: expected append of %s, but it is an overwrite", tc.tableName, i, columnName)
					}

					checkList(t, tc.tableName, columnName, i, changeValue, v.values)

					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if !reflect.ValueOf(deletedElements).IsNil() {
						t.Errorf("%s[%d]: expected %s deleted elements to be nil, but is %#v", tc.tableName, i, columnName, deletedElements)
					}

				case listRemoval:
					if isDeleted {
						t.Errorf("%s[%d]: expected removal from of %s, but it is an overwrite", tc.tableName, i, columnName)
					}
					if !reflect.ValueOf(changeValue).IsNil() {
						t.Errorf("%s[%d]: expected elements of %s to not be present", tc.tableName, i, columnName)
					}
					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if reflect.ValueOf(deletedElements).Len() != v.elementCount {
						t.Errorf("%s[%d]: expected %s deleted elements to have %d elements, but has %d", tc.tableName, i, columnName, v.elementCount, reflect.ValueOf(deletedElements).Len())
					}

				case udtSetFields:
					if isDeleted {
						t.Errorf("%s[%d]: expected udt fields set/remove from %s, but it is an overwrite", tc.tableName, i, columnName)
					}
					if !reflect.DeepEqual(changeValue, v.set) {
						t.Errorf("%s[%d]: expected values set in %s to be %#v, but they are %#v", tc.tableName, i, columnName, v.set, changeValue)
					}
					if !hasDeletedElements {
						t.Errorf("%s[%d]: expected %s to have deleted elements column", tc.tableName, i, columnName)
					} else if !reflect.DeepEqual(deletedElements, v.removed) {
						t.Errorf("%s[%d]: expected %s deleted elements to be %#v, but is %#v", tc.tableName, i, columnName, v.removed, deletedElements)
					}

				case notSet:
					// TODO
				}
			}
		}
	}
}

func checkList(t *testing.T, tableName string, columnName string, rowNum int, actual interface{}, expected interface{}) {
	// Sort values by their timeuuid
	type cell struct {
		key   gocql.UUID
		value interface{}
	}

	var asList []cell
	iter := reflect.ValueOf(actual).MapRange()
	for iter.Next() {
		asList = append(asList, cell{
			key:   iter.Key().Interface().(gocql.UUID),
			value: iter.Value().Interface(),
		})
	}

	sort.Slice(asList, func(i, j int) bool {
		return compareTimeuuid(asList[i].key, asList[j].key) < 0
	})

	rExpected := reflect.ValueOf(expected)
	if len(asList) != rExpected.Len() {
		t.Errorf("%s[%d]: expected %s to have %d elements, but has %d", tableName, rowNum, columnName, rExpected.Len(), len(asList))
	} else {
		for j := 0; j < rExpected.Len(); j++ {
			el := rExpected.Index(j).Interface()
			if !reflect.DeepEqual(asList[j].value, el) {
				t.Errorf("%s[%d]: expected value at index %d in %s to be %v, but is %v", tableName, rowNum, j, columnName, el, asList[j].value)
			}
		}
	}
}

func execQuery(t *testing.T, session *gocql.Session, query string) {
	t.Logf("executing query %s", query)
	err := session.Query(query).Exec()
	if err != nil {
		t.Fatal(err)
	}
}

func ptrTo(v interface{}) interface{} {
	vptr := reflect.New(reflect.TypeOf(v))
	reflect.Indirect(vptr).Set(reflect.ValueOf(v))
	return vptr.Interface()
}

package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/gocql/gocql"
	scylla_cdc "github.com/piodul/scylla-cdc-go"
)

const (
	sourceAddress      = "127.0.0.1"
	destinationAddress = "127.0.0.2"
)

type schema struct {
	tableName   string
	createQuery string
}

var udts = []string{
	"CREATE TYPE ks.udt_simple (a int, b int, c text)",
}

var (
	schemaSimple = schema{
		"ks.tbl_simple",
		"CREATE TABLE ks.tbl_simple (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck))",
	}
	schemaMultipleClusteringKeys = schema{
		"ks.tbl_multiple_clustering_keys",
		"CREATE TABLE ks.tbl_multiple_clustering_keys (pk text, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2))",
	}
	schemaBlobs = schema{
		"ks.tbl_blobs",
		"CREATE TABLE ks.tbl_blobs (pk text, ck int, v blob, PRIMARY KEY (pk, ck))",
	}
	schemaLists = schema{
		"ks.tbl_lists",
		"CREATE TABLE ks.tbl_lists (pk text, ck int, v list<int>, PRIMARY KEY(pk, ck))",
	}
	schemaSets = schema{
		"ks.tbl_sets",
		"CREATE TABLE ks.tbl_sets (pk text, ck int, v set<int>, PRIMARY KEY (pk, ck))",
	}
	schemaMaps = schema{
		"ks.tbl_maps",
		"CREATE TABLE ks.tbl_maps (pk text, ck int, v map<int, int>, PRIMARY KEY (pk, ck))",
	}
	schemaTuples = schema{
		"ks.tbl_tuples",
		"CREATE TABLE ks.tbl_tuples (pk text, ck int, v tuple<int, text>, PRIMARY KEY (pk, ck))",
	}
	schemaTuplesInTuples = schema{
		"ks.tbl_tuples_in_tuples",
		"CREATE TABLE ks.tbl_tuples_in_tuples (pk text, ck int, v tuple<tuple<int, text>, int>, PRIMARY KEY (pk, ck))",
	}
	schemaTuplesInTuplesInTuples = schema{
		"ks.tbl_tuples_in_tuples_in_tuples",
		"CREATE TABLE ks.tbl_tuples_in_tuples_in_tuples (pk text, ck int, v tuple<tuple<tuple<int, int>, text>, int>, PRIMARY KEY (pk, ck))",
	}
	schemaUDTs = schema{
		"ks.tbl_udts",
		"CREATE TABLE ks.tbl_udts (pk text, ck int, v ks.udt_simple, PRIMARY KEY (pk, ck))",
	}
)

var testCases = []struct {
	schema  schema
	pk      string
	queries []string
}{
	// Operations test cases
	{
		schemaSimple,
		"simpleInserts",
		[]string{
			"INSERT INTO %s (pk, ck, v1, v2) VALUES ('simpleInserts', 1, 2, 'abc')",
			"INSERT INTO %s (pk, ck, v1) VALUES ('simpleInserts', 2, 3)",
			"INSERT INTO %s (pk, ck, v2) VALUES ('simpleInserts', 2, 'def')",
		},
	},
	{
		schemaSimple,
		"simpleUpdates",
		[]string{
			"UPDATE %s SET v1 = 1 WHERE pk = 'simpleUpdates' AND ck = 1",
			"UPDATE %s SET v2 = 'abc' WHERE pk = 'simpleUpdates' AND ck = 2",
			"UPDATE %s SET v1 = 5, v2 = 'def' WHERE pk = 'simpleUpdates' AND ck = 3",
		},
	},
	{
		schemaSimple,
		"rowDeletes",
		[]string{
			"INSERT INTO %s (pk, ck, v1, v2) VALUES ('rowDeletes', 1, 2, 'abc')",
			"INSERT INTO %s (pk, ck, v1, v2) VALUES ('rowDeletes', 2, 3, 'def')",
			"DELETE FROM %s WHERE pk = 'rowDeletes' AND ck = 1",
		},
	},
	{
		schemaSimple,
		"partitionDeletes",
		[]string{
			"INSERT INTO %s (pk, ck, v1, v2) VALUES ('partitionDeletes', 1, 2, 'abc')",
			"INSERT INTO %s (pk, ck, v1, v2) VALUES ('partitionDeletes', 2, 3, 'def')",
			"DELETE FROM %s WHERE pk = 'partitionDeletes'",
			// Insert one more row, just to check if replication works at all
			"INSERT INTO %s (pk, ck, v1, v2) VALUES ('partitionDeletes', 4, 5, 'def')",
		},
	},
	{
		schemaMultipleClusteringKeys,
		"rangeDeletes",
		[]string{
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 1, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 2, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 3, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 1, 4, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 1, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 2, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 3, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 2, 4, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 1, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 2, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 3, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 3, 4, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 1, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 2, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 3, 0)",
			"INSERT INTO %s (pk, ck1, ck2, v) VALUES ('rangeDeletes', 4, 4, 0)",
			"DELETE FROM %s WHERE pk = 'rangeDeletes' AND ck1 > 3",
			"DELETE FROM %s WHERE pk = 'rangeDeletes' AND ck1 <= 1",
			"DELETE FROM %s WHERE pk = 'rangeDeletes' AND ck1 = 2 AND ck2 > 1 AND ck2 < 4",
		},
	},

	// Blob test cases
	{
		schemaBlobs,
		"blobs",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('blobs', 1, 0x1234)",
			"INSERT INTO %s (pk, ck, v) VALUES ('blobs', 2, 0x)",
			"INSERT INTO %s (pk, ck, v) VALUES ('blobs', 3, null)",
			"INSERT INTO %s (pk, ck, v) VALUES ('blobs', 4, 0x4321)",
			"INSERT INTO %s (pk, ck, v) VALUES ('blobs', 5, 0x00)",
			"UPDATE %s SET v = null WHERE pk = 'blobs' AND ck = 4",
			"UPDATE %s SET v = 0x WHERE pk = 'blobs' AND ck = 5",
		},
	},

	// Lists test cases
	{
		schemaLists,
		"listOverwrites",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 1, [1, 2, 3])",
			"INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 1, [4, 5, 6, 7])",
			"INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 2, [6, 5, 4, 3, 2, 1])",
			"INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 2, null)",
			"INSERT INTO %s (pk, ck, v) VALUES ('listOverwrites', 3, [1, 11, 111])",
			"UPDATE %s SET v = [2, 22, 222] WHERE pk = 'listOverwrites' AND ck = 3",
		},
	},
	{
		schemaLists,
		"listAppends",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('listAppends', 1, [1, 2, 3])",
			"UPDATE %s SET v = v + [4, 5, 6] WHERE pk = 'listAppends' AND ck = 1",
			"UPDATE %s SET v = [-2, -1, 0] + v WHERE pk = 'listAppends' AND ck = 1",
		},
	},
	{
		schemaLists,
		"listRemoves",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('listRemoves', 1, [1, 2, 3])",
			"UPDATE %s SET v = v + [4, 5, 6] WHERE pk = 'listRemoves' AND ck = 1",
			"UPDATE %s SET v = v - [1, 2, 3] WHERE pk = 'listRemoves' AND ck = 1",
		},
	},

	// Set test cases
	{
		schemaSets,
		"setOverwrites",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 1, {1, 2, 3, 4})",
			"INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 1, {4, 5, 6, 7})",
			"INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 2, {8, 9, 10, 11})",
			"INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 2, null)",
			"INSERT INTO %s (pk, ck, v) VALUES ('setOverwrites', 3, {12, 13, 14, 15})",
			"UPDATE %s SET v = null WHERE pk = 'setOverwrites' AND ck = 3",
		},
	},
	{
		schemaSets,
		"setAppends",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('setAppends', 1, {1, 2, 3, 4})",
			"UPDATE %s SET v = v + {5, 6} WHERE pk = 'setAppends' AND ck = 1",
			"UPDATE %s SET v = v + {5, 6} WHERE pk = 'setAppends' AND ck = 2",
		},
	},
	{
		schemaSets,
		"setRemovals",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('setRemovals', 1, {1, 2, 3, 4})",
			"UPDATE %s SET v = v - {1, 3} WHERE pk = 'setRemovals' AND ck = 1",
			"UPDATE %s SET v = v - {1138} WHERE pk = 'setRemovals' AND ck = 2",
		},
	},

	// Map test cases
	{
		schemaMaps,
		"mapOverwrites",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 1, {1: 2, 3: 4})",
			"INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 1, {5: 6, 7: 8})",
			"INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 2, {9: 10, 11: 12})",
			"INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 2, null)",
			"INSERT INTO %s (pk, ck, v) VALUES ('mapOverwrites', 3, {13: 14, 15: 16})",
			"UPDATE %s SET v = null WHERE pk = 'mapOverwrites' AND ck = 3",
		},
	},
	{
		schemaMaps,
		"mapSets",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('mapSets', 1, {1: 2, 3: 4, 5: 6})",
			"UPDATE %s SET v[1] = 42 WHERE pk = 'mapSets' AND ck = 1",
			"UPDATE %s SET v[3] = null WHERE pk = 'mapSets' AND ck = 1",
			"UPDATE %s SET v[3] = 123 WHERE pk = 'mapSets' AND ck = 1",
			"UPDATE %s SET v[5] = 321 WHERE pk = 'mapSets' AND ck = 2",
		},
	},
	{
		schemaMaps,
		"mapAppends",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('mapAppends', 1, {1: 2, 3: 4})",
			"UPDATE %s SET v = v + {5: 6} WHERE pk = 'mapAppends' AND ck = 1",
			"UPDATE %s SET v = v + {5: 6} WHERE pk = 'mapAppends' AND ck = 2",
		},
	},
	{
		schemaMaps,
		"mapRemovals",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('mapRemovals', 1, {1: 2, 3: 4})",
			"UPDATE %s SET v = v - {1} WHERE pk = 'mapRemovals' AND ck = 1",
			"UPDATE %s SET v = v - {1138} WHERE pk = 'mapRemovals' AND ck = 2",
		},
	},

	// Tuple test cases
	{
		schemaTuples,
		"tupleInserts",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleInserts', 1, (7, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleInserts', 2, (9, 'def'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleInserts', 2, null)",
		},
	},
	{
		schemaTuples,
		"tupleUpdates",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 1, (7, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 2, (9, 'def'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 3, (11, 'ghi'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 4, (13, 'jkl'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 5, (15, 'mno'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 6, (17, 'pqr'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tupleUpdates', 7, (19, 'stu'))",
			"UPDATE %s SET v = (111, 'zyx') WHERE pk = 'tupleUpdates' AND ck = 1",
			"UPDATE %s SET v = null WHERE pk = 'tupleUpdates' AND ck = 2",
			"INSERT INTO %s (pk, ck) VALUES ('tupleUpdates', 3)",
			"UPDATE %s SET v = (null, null) WHERE pk = 'tupleUpdates' AND ck = 4",
			"UPDATE %s SET v = (null, 'asdf') WHERE pk = 'tupleUpdates' AND ck = 5",
			"UPDATE %s SET v = (123, null) WHERE pk = 'tupleUpdates' AND ck = 6",
			"UPDATE %s SET v = (null, '') WHERE pk = 'tupleUpdates' AND ck = 7",
		},
	},
	{
		schemaTuplesInTuples,
		"tuplesInTuples",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 1, ((1, 'abc'), 7))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 2, ((3, 'def'), 9))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 3, ((3, 'ghi'), 9))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuples', 4, ((3, 'jkl'), 9))",
			"UPDATE %s SET v = ((100, 'zyx'), 111) WHERE pk = 'tuplesInTuples' AND ck = 1",
			"UPDATE %s SET v = null WHERE pk = 'tuplesInTuples' AND ck = 2",
			"UPDATE %s SET v = ((200, null), 999) WHERE pk = 'tuplesInTuples' AND ck = 3",
			"UPDATE %s SET v = ((300, ''), 333) WHERE pk = 'tuplesInTuples' AND ck = 4",
		},
	},
	{
		schemaTuplesInTuplesInTuples,
		"tuplesInTuplesInTuples",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuplesInTuples', 1, (((1, 9), 'abc'), 7))",
			"INSERT INTO %s (pk, ck, v) VALUES ('tuplesInTuplesInTuples', 2, (((3, 8), 'def'), 9))",
			"UPDATE %s SET v = (((100, 200), 'zyx'), 111) WHERE pk = 'tuplesInTuplesInTuples' AND ck = 1",
			"UPDATE %s SET v = null WHERE pk = 'tuplesInTuplesInTuples' AND ck = 2",
			"UPDATE %s SET v = (null, 123) WHERE pk = 'tuplesInTuplesInTuples' AND ck = 3",
			"UPDATE %s SET v = ((null, 'xyz'), 321) WHERE pk = 'tuplesInTuplesInTuples' AND ck = 4",
		},
	},

	// UDT test cases
	{
		schemaUDTs,
		"udt",
		[]string{
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 1, (2, 3, 'abc'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 2, {a: 6, c: 'zxcv'})",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 3, (9, 4, 'def'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 4, (123, 321, 'ghi'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 5, (333, 222, 'jkl'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 6, (432, 678, 'mno'))",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 7, (765, 345, 'pqr'))",
			"UPDATE %s SET v.b = 41414 WHERE pk = 'udt' AND ck = 2",
			"UPDATE %s SET v = null WHERE pk = 'udt' AND ck = 3",
			"UPDATE %s SET v = {b: 123456, c: 'tyu'} WHERE pk = 'udt' AND ck = 4",
			"INSERT INTO %s (pk, ck, v) VALUES ('udt', 5, (999, 888, 'zxc'))",
			"UPDATE %s SET v.c = null WHERE pk = 'udt' AND ck = 6",
			"UPDATE %s SET v = {a: 923, b: 123456, c: ''} WHERE pk = 'udt' AND ck = 7",
		},
	},
}

func TestReplicator(t *testing.T) {
	filter := os.Getenv("REPLICATOR_TEST_FILTER")
	if filter == "" {
		filter = ".*"
	}
	re := regexp.MustCompile(filter)

	// Collect all schemas
	schemas := make(map[string]string)
	for _, tc := range testCases {
		schemas[tc.schema.tableName] = tc.schema.createQuery
	}

	// TODO: Provide IPs from the env
	sourceSession := createSessionAndSetupSchema(t, sourceAddress, true, schemas)
	defer sourceSession.Close()

	destinationSession := createSessionAndSetupSchema(t, destinationAddress, false, schemas)
	defer destinationSession.Close()

	// Execute all of the queries
	for _, tc := range testCases {
		if !re.MatchString(tc.pk) {
			continue
		}
		for _, qStr := range tc.queries {
			execQuery(t, sourceSession, fmt.Sprintf(qStr, tc.schema.tableName))
		}
	}

	t.Log("running replicators")

	adv := scylla_cdc.AdvancedReaderConfig{
		ChangeAgeLimit:         time.Minute,
		PostNonEmptyQueryDelay: 3 * time.Second,
		PostEmptyQueryDelay:    3 * time.Second,
		PostFailedQueryDelay:   3 * time.Second,
		QueryTimeWindowSize:    5 * time.Minute,
		ConfidenceWindowSize:   0,
	}

	schemaNames := make([]string, 0)
	for tbl := range schemas {
		schemaNames = append(schemaNames, tbl)
	}

	replicator, _, err := MakeReplicator(sourceAddress, destinationAddress, schemaNames, &adv, gocql.Quorum)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	errC := make(chan error)
	go func() { errC <- replicator.Run(ctx) }()

	replicator.StopAt(time.Now().Add(time.Second))
	if err := <-errC; err != nil {
		t.Fatal(err)
	}

	t.Log("validating results")

	// Compare
	sourceSet := fetchFullSet(t, sourceSession, schemas)
	destinationSet := fetchFullSet(t, destinationSession, schemas)

	failedCount := 0

	for _, tc := range testCases {
		sourceData := sourceSet[tc.pk]
		destinationData := destinationSet[tc.pk]

		if len(sourceData) != len(destinationData) {
			t.Logf(
				"%s: source len %d, destination len %d\n",
				tc.pk,
				len(sourceData),
				len(destinationData),
			)
			t.Log("  source:")
			for _, row := range sourceData {
				t.Logf("    %v", row)
			}
			t.Log("  dest:")
			for _, row := range destinationData {
				t.Logf("    %v", row)
			}
			t.Fail()
			failedCount++
			continue
		}

		failed := false
		for i := 0; i < len(sourceData); i++ {
			if !reflect.DeepEqual(sourceData[i], destinationData[i]) {
				t.Logf("%s: mismatch", tc.pk)
				t.Logf("  source: %v", sourceData[i])
				t.Logf("  dest:   %v", destinationData[i])
				failed = true
			}
		}

		if failed {
			t.Fail()
			failedCount++
		} else {
			t.Logf("%s: OK", tc.pk)
		}
	}

	if failedCount > 0 {
		t.Logf("failed %d/%d test cases", failedCount, len(testCases))
	}
}

func createSessionAndSetupSchema(t *testing.T, addr string, withCdc bool, schemas map[string]string) *gocql.Session {
	cfg := gocql.NewCluster(addr)
	session, err := cfg.CreateSession()
	if err != nil {
		t.Fatal(err)
	}

	execQuery(t, session, "DROP KEYSPACE IF EXISTS ks")
	execQuery(t, session, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

	for _, udt := range udts {
		execQuery(t, session, udt)
	}

	for _, tbl := range schemas {
		tblQuery := tbl
		if withCdc {
			tblQuery += " WITH cdc = {'enabled': true}"
		}
		execQuery(t, session, tblQuery)
	}

	err = session.AwaitSchemaAgreement(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return session
}

func execQuery(t *testing.T, session *gocql.Session, query string) {
	t.Logf("executing query %s", query)
	err := session.Query(query).Exec()
	if err != nil {
		t.Fatal(err)
	}
}

func fetchFullSet(t *testing.T, session *gocql.Session, schemas map[string]string) map[string][]map[string]interface{} {
	groups := make(map[string][]map[string]interface{})

	for tbl := range schemas {
		data, err := session.Query("SELECT * FROM " + tbl).Iter().SliceMap()
		if err != nil {
			t.Fatal(err)
		}

		for _, row := range data {
			pk := row["pk"].(string)
			groups[pk] = append(groups[pk], row)
		}
	}

	return groups
}

package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

const (
	schemaSimple = "CREATE TABLE ks.tbl (pk text, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck))"
)

var testCases = []struct {
	schema  string
	pk      string
	queries []string
}{
	{
		schemaSimple,
		"simpleInserts",
		[]string{
			"INSERT INTO ks.tbl (pk, ck, v1, v2) VALUES ('simpleInserts', 1, 2, 'abc')",
			"INSERT INTO ks.tbl (pk, ck, v1) VALUES ('simpleInserts', 2, 3)",
			"INSERT INTO ks.tbl (pk, ck, v2) VALUES ('simpleInserts', 2, 'def')",
		},
	},
}

func TestReplicator(t *testing.T) {
	// Collect all schemas
	schemas := make(map[string]struct{})
	for _, tc := range testCases {
		schemas[tc.schema] = struct{}{}
	}

	// Create all the schemas in the source cluster
	// TODO: Provide IPs from the env
	cfgSource := gocql.NewCluster("127.0.0.1")
	sourceSession, err := cfgSource.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer sourceSession.Close()

	err = sourceSession.Query("DROP KEYSPACE IF EXISTS ks").Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = sourceSession.Query("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec()
	if err != nil {
		t.Fatal(err)
	}

	for tbl := range schemas {
		err = sourceSession.Query(tbl + " WITH cdc = {'enabled': true}").Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create all the schemas in the destination cluster
	cfgDestination := gocql.NewCluster("127.0.0.2")
	destinationSession, err := cfgDestination.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer destinationSession.Close()

	err = destinationSession.Query("DROP KEYSPACE IF EXISTS ks").Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = destinationSession.Query("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec()
	if err != nil {
		t.Fatal(err)
	}

	for tbl := range schemas {
		err = destinationSession.Query(tbl).Exec()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait until schema agreement
	err = sourceSession.AwaitSchemaAgreement(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = destinationSession.AwaitSchemaAgreement(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Execute all of the queries
	for _, tc := range testCases {
		for _, qStr := range tc.queries {
			err := sourceSession.Query(qStr).Exec()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	finishF, err := RunReplicator(context.Background(), "ks", "tbl", "127.0.0.1", "127.0.0.2")
	if err != nil {
		t.Fatal(err)
	}

	// Wait 10 seconds
	<-time.After(10 * time.Second)
	err = finishF()
	if err != nil {
		t.Fatal(err)
	}

	// Compare
	sourceSet := fetchFullSet(t, sourceSession)
	destinationSet := fetchFullSet(t, destinationSession)

	for _, tc := range testCases {
		sourceData := sourceSet[tc.pk]
		destinationData := destinationSet[tc.pk]

		if len(sourceData) != len(destinationData) {
			t.Fatalf(
				"%s: source len %d, destination len %d\n",
				tc.pk,
				len(sourceData),
				len(destinationData),
			)
		}

		for i := 0; i < len(sourceData); i++ {
			if !reflect.DeepEqual(sourceData[i], destinationData[i]) {
				t.Logf("%s: mismatch", tc.pk)
				t.Logf("  source: %v", sourceData[i])
				t.Logf("  dest:   %v", destinationData[i])
				t.FailNow()
			}
		}
	}
}

func fetchFullSet(t *testing.T, session *gocql.Session) map[string][]map[string]interface{} {
	data, err := session.Query("SELECT * FROM ks.tbl").Iter().SliceMap()
	if err != nil {
		t.Fatal(err)
	}

	groups := make(map[string][]map[string]interface{})
	for _, row := range data {
		pk := row["pk"].(string)
		groups[pk] = append(groups[pk], row)
	}
	return groups
}

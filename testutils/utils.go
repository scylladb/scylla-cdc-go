package testutils

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

var (
	testStartTimestamp time.Time
	currentTestNumber  uint32
)

func init() {
	testStartTimestamp = time.Now()
}

func GetUniqueName(prefix string) string {
	unixNano := testStartTimestamp.UnixNano()
	uniqueID := atomic.AddUint32(&currentTestNumber, 1) - 1
	return fmt.Sprintf("%s_%d_%d", prefix, unixNano, uniqueID)
}

func GetSourceClusterContactPoint() string {
	uri := os.Getenv("SCYLLA_SRC_URI")
	if uri == "" {
		uri = "127.0.0.1"
	}
	return uri
}

func GetDestinationClusterContactPoint() string {
	uri := os.Getenv("SCYLLA_DST_URI")
	if uri == "" {
		uri = "127.0.0.2"
	}
	return uri
}

func CreateKeyspace(t *testing.T, contactPoint, keyspaceName string, tabletsEnabled bool) {
	t.Helper()

	cluster := gocql.NewCluster(contactPoint)
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	defer session.Close()

	tabletsOption := "false"
	if tabletsEnabled {
		tabletsOption = "true"
	}

	query := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'} AND tablets={'enabled': %s}", keyspaceName, tabletsOption)
	err = session.Query(query).Exec()
	if err != nil {
		t.Fatalf("failed to create keyspace %s: %v", keyspaceName, err)
	}

	err = session.AwaitSchemaAgreement(context.Background())
	if err != nil {
		t.Fatalf("awaiting schema agreement failed: %v", err)
	}

	// Check if CDC is supported by attempting to create a test table with CDC enabled
	testTableName := "cdc_support_test"
	cdcTestQuery := fmt.Sprintf("CREATE TABLE %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", keyspaceName, testTableName)
	err = session.Query(cdcTestQuery).Exec()
	if err != nil {
		t.Skipf("CDC is not supported on this cluster: %v", err)
		return
	}

	// Clean up the test table
	dropTestQuery := fmt.Sprintf("DROP TABLE %s.%s", keyspaceName, testTableName)
	_ = session.Query(dropTestQuery).Exec() // Ignore errors on cleanup
}

func CreateUniqueKeyspace(t *testing.T, contactPoint string, tabletsEnabled bool) string {
	t.Helper()

	keyspaceName := GetUniqueName("test_keyspace")
	CreateKeyspace(t, contactPoint, keyspaceName, tabletsEnabled)
	return keyspaceName
}

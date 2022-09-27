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
	currentTestNumber  uint32 = 0
)

func init() {
	testStartTimestamp = time.Now()
}

func GetUniqueName(prefix string) string {
	unixNano := testStartTimestamp.UnixNano()
	uniqueId := atomic.AddUint32(&currentTestNumber, 1) - 1
	return fmt.Sprintf("%s_%d_%d", prefix, unixNano, uniqueId)
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

func CreateKeyspace(t *testing.T, contactPoint string, keyspaceName string) {
	cluster := gocql.NewCluster(contactPoint)
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	defer session.Close()
	err = session.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspaceName)).Exec()
	if err != nil {
		t.Fatalf("failed to create keyspace %s: %v", keyspaceName, err)
	}

	err = session.AwaitSchemaAgreement(context.Background())
	if err != nil {
		t.Fatalf("awaiting schema agreement failed: %v", err)
	}
}

func CreateUniqueKeyspace(t *testing.T, contactPoint string) string {
	keyspaceName := GetUniqueName("test_keyspace")
	CreateKeyspace(t, contactPoint, keyspaceName)
	return keyspaceName
}

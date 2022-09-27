package scyllacdc

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/scylla-cdc-go/internal/testutils"
)

type recordingConsumer struct {
	mu              *sync.Mutex
	emptyTimestamps []gocql.UUID
}

func (rc *recordingConsumer) CreateChangeConsumer(
	ctx context.Context,
	input CreateChangeConsumerInput,
) (ChangeConsumer, error) {
	return rc, nil
}

func (rc *recordingConsumer) Consume(ctx context.Context, change Change) error {
	return nil
}

func (rc *recordingConsumer) End() error {
	return nil
}

func (rc *recordingConsumer) Empty(ctx context.Context, ackTime gocql.UUID) error {
	rc.mu.Lock()
	rc.emptyTimestamps = append(rc.emptyTimestamps, ackTime)
	rc.mu.Unlock()
	return nil
}

func (rc *recordingConsumer) GetTimestamps() []gocql.UUID {
	rc.mu.Lock()
	ret := append([]gocql.UUID{}, rc.emptyTimestamps...)
	rc.mu.Unlock()
	return ret
}

func TestConsumerCallsEmptyCallback(t *testing.T) {
	consumer := &recordingConsumer{mu: &sync.Mutex{}}

	adv := AdvancedReaderConfig{
		ChangeAgeLimit:         -time.Millisecond,
		PostNonEmptyQueryDelay: 100 * time.Millisecond,
		PostEmptyQueryDelay:    100 * time.Millisecond,
		PostFailedQueryDelay:   100 * time.Millisecond,
		QueryTimeWindowSize:    100 * time.Millisecond,
		ConfidenceWindowSize:   time.Millisecond,
	}

	// Configure a session
	address := testutils.GetSourceClusterContactPoint()
	keyspaceName := testutils.CreateUniqueKeyspace(t, address)
	cluster := gocql.NewCluster(address)
	cluster.Keyspace = keyspaceName
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	execQuery(t, session, "CREATE TABLE tbl (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}")

	cfg := &ReaderConfig{
		Session:               session,
		ChangeConsumerFactory: consumer,
		TableNames:            []string{keyspaceName + ".tbl"},
		Advanced:              adv,
		Logger:                log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile),
	}

	startTime := time.Now()

	reader, err := NewReader(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	errC := make(chan error)
	go func() { errC <- reader.Run(context.Background()) }()

	time.Sleep(time.Second)

	endTime := startTime.Add(5 * time.Second)
	reader.StopAt(endTime)
	if err := <-errC; err != nil {
		t.Fatal(err)
	}

	// All timestamps should be roughly between startTime and endTime
	// To adjust for different clock on the scylla node, allow the time
	// to exceed one second
	acceptableStart := startTime.Add(-time.Second)
	acceptableEnd := endTime.Add(time.Second)

	timestamps := consumer.GetTimestamps()

	if len(timestamps) == 0 {
		t.Fatal("no empty event timestamps recorded")
	}

	for _, tstp := range timestamps {
		early := !acceptableStart.Before(tstp.Time())
		late := !tstp.Time().Before(acceptableEnd)
		if early || late {
			t.Errorf("timestamp of empty event %s not in expected range %s, %s",
				tstp.Time(), acceptableStart, acceptableEnd)
		}
	}
}

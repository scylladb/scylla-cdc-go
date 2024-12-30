package scyllacdc_test

import (
	"context"
	"github.com/scylladb/scylla-cdc-go/testutils"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type recordingConsumer struct {
	mu              *sync.Mutex
	emptyTimestamps []gocql.UUID
}

func (rc *recordingConsumer) CreateChangeConsumer(_ context.Context, _ scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	return rc, nil
}

func (rc *recordingConsumer) Consume(ctx context.Context, change scyllacdc.Change) error {
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

	adv := scyllacdc.AdvancedReaderConfig{
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

	cfg := &scyllacdc.ReaderConfig{
		Session:               session,
		ChangeConsumerFactory: consumer,
		TableNames:            []string{keyspaceName + ".tbl"},
		Advanced:              adv,
		Logger:                log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile),
	}

	startTime := time.Now()

	reader, err := scyllacdc.NewReader(context.Background(), cfg)
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

func TestConsumerResumesWithTableBackedProgressReporter(t *testing.T) {
	// Makes sure that the table backed progress consumer is able to resume correctly
	// when StartGeneration was called, but no SaveProgress has been called
	// so far.

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

	runWithProgressReporter := func(consumerFactory scyllacdc.ChangeConsumerFactory, endTime time.Time, adv scyllacdc.AdvancedReaderConfig) {
		progressManager, err := scyllacdc.NewTableBackedProgressManager(session, "progress", "test")
		if err != nil {
			t.Fatalf("failed to create progress manager: %v", err)
		}

		cfg := &scyllacdc.ReaderConfig{
			Session:               session,
			ChangeConsumerFactory: consumerFactory,
			TableNames:            []string{keyspaceName + ".tbl"},
			ProgressManager:       progressManager,
			Advanced:              adv,
			Logger:                log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile),
		}

		reader, err := scyllacdc.NewReader(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}

		errC := make(chan error)
		go func() { errC <- reader.Run(context.Background()) }()

		time.Sleep(500 * time.Millisecond)

		reader.StopAt(endTime)
		if err := <-errC; err != nil {
			t.Fatal(err)
		}
	}

	startTime := time.Now()

	adv := scyllacdc.AdvancedReaderConfig{
		PostNonEmptyQueryDelay: 100 * time.Millisecond,
		PostEmptyQueryDelay:    100 * time.Millisecond,
		PostFailedQueryDelay:   100 * time.Millisecond,
		QueryTimeWindowSize:    100 * time.Millisecond,
		ConfidenceWindowSize:   time.Millisecond,
	}

	// Create and start the first consumer which will not call SaveProgress
	// Start reading from ~now and stop after two seconds
	// We should record that we started now but recorded no progress for
	// any stream
	adv.ChangeAgeLimit = -time.Millisecond
	consumer := &recordingConsumer{mu: &sync.Mutex{}}
	runWithProgressReporter(consumer, startTime.Add(2*time.Second), adv)

	// Create and start the second consumer
	// The progress manager should resume reading from the time
	// when the previous run was started, not 1 minute ago
	adv.ChangeAgeLimit = 10 * time.Second
	consumer = &recordingConsumer{mu: &sync.Mutex{}}
	runWithProgressReporter(consumer, startTime.Add(4*time.Second), adv)

	// All timestamps should be roughly between startTime and endTime
	// To adjust for different clock on the scylla node, allow the time
	// to exceed one second
	acceptableStart := startTime.Add(-time.Second)
	acceptableEnd := startTime.Add(4 * time.Second).Add(time.Second)

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

func TestConsumerHonorsTableTTL(t *testing.T) {
	// Make sure that the library doesn't attempt to read earlier than
	// the table TTL

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

	// Create a table with a very short TTL
	execQuery(t, session, "CREATE TABLE tbl (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true, 'ttl': 2}")

	startTime := time.Now()
	endTime := startTime.Add(2 * time.Second)

	adv := scyllacdc.AdvancedReaderConfig{
		PostNonEmptyQueryDelay: 100 * time.Millisecond,
		PostEmptyQueryDelay:    100 * time.Millisecond,
		PostFailedQueryDelay:   100 * time.Millisecond,
		QueryTimeWindowSize:    500 * time.Millisecond,
		ConfidenceWindowSize:   time.Millisecond,
		ChangeAgeLimit:         time.Minute, // should be overridden by the TTL
	}

	consumer := &recordingConsumer{mu: &sync.Mutex{}}

	cfg := &scyllacdc.ReaderConfig{
		Session:               session,
		ChangeConsumerFactory: consumer,
		TableNames:            []string{keyspaceName + ".tbl"},
		Advanced:              adv,
		Logger:                log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile),
	}

	reader, err := scyllacdc.NewReader(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	errC := make(chan error)
	go func() { errC <- reader.Run(context.Background()) }()

	time.Sleep(500 * time.Millisecond)

	reader.StopAt(endTime)
	if err := <-errC; err != nil {
		t.Fatal(err)
	}

	// All timestamps should be roughly between startTime-TTL and endTime
	// To adjust for different clock on the scylla node, allow the time
	// to exceed one second
	acceptableStart := startTime.Add(-time.Second).Add(-2 * time.Second)
	acceptableEnd := startTime.Add(2 * time.Second).Add(time.Second)

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

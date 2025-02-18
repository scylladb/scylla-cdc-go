package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gocql/gocql"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

// TODO: Escape field names?
var showTimestamps = false

var reportPeriod = 1 * time.Minute

func main() {
	var (
		keyspace         string
		table            string
		source           string
		progressNode     string
		readConsistency  string
		writeConsistency string

		pubTopic string

		progressTable string
	)

	flag.StringVar(&keyspace, "keyspace", "", "keyspace name")
	flag.StringVar(&table, "table", "", "table name; you can specify multiple table by separating them with a comma")
	flag.StringVar(&source, "source", "", "address of a node in source cluster")
	flag.StringVar(&progressNode, "progress-node", "", "address of a node in progress cluster")
	flag.StringVar(&readConsistency, "read-consistency", "", "consistency level used to read from cdc log (one, quorum, all)")
	flag.StringVar(&writeConsistency, "write-consistency", "", "consistency level used to write to the destination cluster (one, quorum, all)")
	flag.StringVar(&progressTable, "progress-table", "", "fully-qualified name of the table in the destination cluster to use for saving progress; if omitted, the progress won't be saved")

	flag.StringVar(&pubTopic, "topic", "", "GCP PUB/SUB Topic")

	flag.String("mode", "", "mode (ignored)")

	adv := scyllacdc.AdvancedReaderConfig{}
	flag.DurationVar(&adv.ConfidenceWindowSize, "polling-confidence-window-size", 30*time.Second, "defines a minimal age a change must have in order to be read.")
	flag.DurationVar(&adv.ChangeAgeLimit, "polling-change-age-limit", 10*time.Minute, "When the library starts for the first time it has to start consuming\nchanges from some point in time. This parameter defines how far in the\npast it needs to look. If the value of the parameter is set to an hour,\nthen the library will only read historical changes that are no older than\nan hour.")
	flag.DurationVar(&adv.QueryTimeWindowSize, "pooling-query-time-window-size", 1*time.Minute, "Changes are queried using select statements with restriction on the time\nthose changes appeared. The restriction is bounding the time from both\nlower and upper bounds. This parameter defines the width of the time\nwindow used for the restriction.")
	flag.DurationVar(&adv.PostEmptyQueryDelay, "polling-post-empty-query-delay", 30*time.Second, "The library uses select statements to fetch changes from CDC Log tables.\nEach select fetches changes from a single table and fetches only changes\nfrom a limited set of CDC streams. If such select returns no changes then\nnext select to this table and set of CDC streams will be issued after\na delay. This parameter specifies the length of the delay")
	flag.DurationVar(&adv.PostNonEmptyQueryDelay, "polling-post-non-empty-query-delay", 10*time.Second, "The library uses select statements to fetch changes from CDC Log tables.\nEach select fetches changes from a single table and fetches only changes\nfrom a limited set of CDC streams. If such select returns one or more\nchanges then next select to this table and set of CDC streams will be\nissued after a delay. This parameter specifies the length of the delay")
	flag.DurationVar(&adv.PostFailedQueryDelay, "pooling-post-failed-query-delay", 1*time.Second, "If the library tries to read from the CDC log and the read operation\nfails, it will wait some time before attempting to read again. This\nparameter specifies the length of the delay.")

	flag.Parse()

	clRead := parseConsistency(readConsistency)
	clWrite := parseConsistency(writeConsistency)

	fmt.Println("Parameters:")
	fmt.Printf("  Keyspace: %s\n", keyspace)
	fmt.Printf("  Table: %s\n", table)
	fmt.Printf("  Source cluster IP: %s\n", source)
	fmt.Printf("  Destination cluster IP: %s\n", progressNode)
	fmt.Printf("  Consistency for reads: %s\n", clRead)
	fmt.Printf("  Consistency for writes: %s\n", clWrite)
	fmt.Printf("  Table to use for saving progress: %s\n", progressTable)
	fmt.Println("Advanced reader parameters:")
	fmt.Printf("  Confidence window size: %s\n", adv.ConfidenceWindowSize)
	fmt.Printf("  Change age limit: %s\n", adv.ChangeAgeLimit)
	fmt.Printf("  Query window size: %s\n", adv.QueryTimeWindowSize)
	fmt.Printf("  Delay after poll with empty results: %s\n", adv.PostEmptyQueryDelay)
	fmt.Printf("  Delay after poll with non-empty results: %s\n", adv.PostNonEmptyQueryDelay)
	fmt.Printf("  Delay after failed poll: %s\n", adv.PostFailedQueryDelay)

	var fullyQualifiedTables []string

	for _, t := range strings.Split(table, ",") {
		fullyQualifiedTables = append(fullyQualifiedTables, keyspace+"."+t)
	}

	logger := log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	repl, err := newReplicator(
		context.Background(),
		source, progressNode,
		fullyQualifiedTables,
		pubTopic,
		&adv,
		clRead,
		clWrite,
		progressTable,
		logger,
	)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// React to Ctrl+C signal.
	//
	// 1st signal will cause the replicator to read changes up until
	// the moment the signal was received, and then it will stop the replicator.
	// This is the "most graceful" way of stopping the replicator.
	//
	// 2nd signal will cancel the context. This should stop all operations
	// done by the replicator ASAP and stop it.
	//
	// 3rd signal will exit the process immediately with error code 1.
	signalC := make(chan os.Signal, 3)
	go func() {
		<-signalC
		now := time.Now()
		log.Printf("stopping at %v", now)
		repl.StopAt(now)

		<-signalC
		log.Printf("stopping now")
		cancel()

		<-signalC
		log.Printf("killing")
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	if err := repl.Run(ctx); err != nil {
		log.Fatalln(err)
	}

	log.Printf("quitting, rows read: %d", repl.GetReadRowsCount())
}

func parseConsistency(s string) gocql.Consistency {
	switch strings.ToLower(s) {
	case "one":
		return gocql.One
	case "quorum":
		return gocql.Quorum
	case "all":
		return gocql.All
	default:
		log.Printf("warning: got unsupported consistency level \"%s\", will use \"one\" instead", s)
		return gocql.One
	}
}

type replicator struct {
	reader *scyllacdc.Reader

	readerSession   *gocql.Session
	progressSession *gocql.Session

	rowsRead *int64
}

func newReplicator(
	ctx context.Context,
	source, destination string,
	tableNames []string,
	topic string,
	advancedParams *scyllacdc.AdvancedReaderConfig,
	readConsistency gocql.Consistency,
	progressConsistency gocql.Consistency,
	progressTable string,
	logger scyllacdc.Logger,
) (*replicator, error) {
	ptCluster := gocql.NewCluster(destination)
	ptCluster.Timeout = 10 * time.Second
	ptCluster.Consistency = progressConsistency
	progressSession, err := ptCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	// Configure a session
	readerCluster := gocql.NewCluster(source)
	readerCluster.Timeout = 10 * time.Second
	readerCluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	readerSession, err := readerCluster.CreateSession()
	if err != nil {
		progressSession.Close()
		return nil, err
	}

	rowsRead := new(int64)

	factory := &replicatorFactory{
		rowsRead: rowsRead,
		topic:    topic,
		logger:   logger,
	}

	var progressManager scyllacdc.ProgressManager
	if progressTable != "" {
		progressManager, err = scyllacdc.NewTableBackedProgressManager(progressSession, progressTable, "cdc-replicator")
		if err != nil {
			progressSession.Close()
			return nil, err
		}
	}

	cfg := &scyllacdc.ReaderConfig{
		Session:               readerSession,
		ChangeConsumerFactory: factory,
		ProgressManager:       progressManager,
		TableNames:            tableNames,
		Consistency:           readConsistency,
	}

	if advancedParams != nil {
		cfg.Advanced = *advancedParams
	}
	cfg.Consistency = readConsistency
	cfg.Logger = logger

	reader, err := scyllacdc.NewReader(ctx, cfg)
	if err != nil {
		readerSession.Close()
		progressSession.Close()
		return nil, err
	}

	repl := &replicator{
		reader: reader,

		readerSession:   readerSession,
		progressSession: progressSession,

		rowsRead: rowsRead,
	}

	return repl, nil
}

func (repl *replicator) Run(ctx context.Context) error {
	defer repl.progressSession.Close()
	defer repl.readerSession.Close()
	return repl.reader.Run(ctx)
}

func (repl *replicator) StopAt(at time.Time) {
	repl.reader.StopAt(at)
}

func (repl *replicator) Stop() {
	repl.reader.Stop()
}

func (repl *replicator) GetReadRowsCount() int64 {
	return atomic.LoadInt64(repl.rowsRead)
}

type replicatorFactory struct {
	rowsRead  *int64
	projectID string
	topic     string
	logger    scyllacdc.Logger
}

func (rf *replicatorFactory) CreateChangeConsumer(
	ctx context.Context,
	input scyllacdc.CreateChangeConsumerInput,
) (scyllacdc.ChangeConsumer, error) {
	splitTableName := strings.SplitN(input.TableName, ".", 2)
	if len(splitTableName) < 2 {
		return nil, fmt.Errorf("table name is not fully qualified: %s", input.TableName)
	}
	return NewPUBReplicator(ctx, rf.projectID, rf.topic, rf.rowsRead, input.StreamID, input.ProgressReporter, rf.logger)
}

type PUBReplicator struct {
	topic       *pubsub.Topic
	pubTopic    string
	consistency gocql.Consistency

	pkColumns    []string
	ckColumns    []string
	otherColumns []string
	columnTypes  map[string]TypeInfo
	allColumns   []string

	insertStr               string
	rowDeleteQueryStr       string
	partitionDeleteQueryStr string

	localCount int64
	totalCount *int64

	streamID scyllacdc.StreamID
	reporter *scyllacdc.PeriodicProgressReporter
}

var globalGCPPubSubClient atomic.Pointer[pubsub.Client]
var globalGCPPubSubClientMutex sync.Mutex

func NewPUBReplicator(
	ctx context.Context,
	projectID, topic string,
	count *int64,
	streamID scyllacdc.StreamID,
	reporter *scyllacdc.ProgressReporter,
	logger scyllacdc.Logger,
) (*PUBReplicator, error) {
	cl := globalGCPPubSubClient.Load()
	if cl == nil {
		var err error
		globalGCPPubSubClientMutex.Lock()
		defer globalGCPPubSubClientMutex.Unlock()
		cl = globalGCPPubSubClient.Load()
		if cl == nil {
			cl, err = pubsub.NewClient(ctx, projectID)
			if err != nil {
				return nil, err
			}

			globalGCPPubSubClient.Store(cl)
		}
	}

	dr := &PUBReplicator{
		topic:      cl.Topic(topic),
		pubTopic:   topic,
		totalCount: count,
		streamID:   streamID,
		reporter:   scyllacdc.NewPeriodicProgressReporter(logger, reportPeriod, reporter),
	}

	dr.reporter.Start(ctx)
	return dr, nil
}

func (r *PUBReplicator) Consume(ctx context.Context, c scyllacdc.Change) error {
	timestamp := c.GetCassandraTimestamp()
	if showTimestamps {
		log.Printf("[%s] Processing timestamp: %s (%s)\n", c.StreamID, c.Time, c.Time.Time())
	}
	wg := &sync.WaitGroup{}
	errs := &writeSafeList[error]{}
	for _, change := range c.Delta {
		wg.Add(1)
		if err := r.sendChangeToPUB(ctx, change, timestamp, "Delta", wg, errs); err != nil {
			return err
		}
	}

	for _, change := range c.PreImage {
		wg.Add(1)
		if err := r.sendChangeToPUB(ctx, change, timestamp, "PreImage", wg, errs); err != nil {
			return err
		}
	}

	for _, change := range c.PostImage {
		wg.Add(1)
		if err := r.sendChangeToPUB(ctx, change, timestamp, "PostImage", wg, errs); err != nil {
			return err
		}
	}

	wg.Wait()
	for _, err := range errs.list {
		if err != nil {
			return err
		}
	}

	r.reporter.Update(c.Time)
	r.localCount += int64(len(c.Delta))

	return nil
}

func (r *PUBReplicator) sendChangeToPUB(ctx context.Context, change *scyllacdc.ChangeRow, timestamp int64, recType string, wg *sync.WaitGroup, errs *writeSafeList[error]) error {
	change.GetRawData()
	change.GetOperation()

	msg, err := json.Marshal(map[string]interface{}{
		"type":         recType,
		"operation":    change.GetOperation(),
		"timestamp":    timestamp,
		"ttl":          change.GetTTL(),
		"seq_no":       change.GetSeqNo(),
		"end_of_batch": change.GetEndOfBatch(),
		"data":         change.GetRawData(),
	})
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	resp := r.topic.Publish(ctx, &pubsub.Message{
		Data: msg,
	})

	go func() {
		defer wg.Done()
		_, err := resp.Get(ctx)
		errs.Add(err)
	}()

	return nil
}

func (r *PUBReplicator) End() error {
	log.Printf("Streams [%s]: processed %d changes in total", r.streamID, r.localCount)
	atomic.AddInt64(r.totalCount, r.localCount)
	_ = r.reporter.SaveAndStop(context.Background())
	r.topic.Flush()
	r.topic.Stop()
	return nil
}

func (r *PUBReplicator) Empty(ctx context.Context, ackTime gocql.UUID) error {
	log.Printf("Streams [%s]: saw no changes up to %s", r.streamID, ackTime.Time())
	r.reporter.Update(ackTime)
	return nil
}

// Make sure that PUBReplicator supports the ChangeOrEmptyNotificationConsumer interface
var _ scyllacdc.ChangeOrEmptyNotificationConsumer = (*PUBReplicator)(nil)

type writeSafeList[V any] struct {
	list  []V
	mutex sync.Mutex
}

func (w *writeSafeList[V]) Add(item V) {
	w.mutex.Lock()
	w.list = append(w.list, item)
	w.mutex.Unlock()
}

func (w *writeSafeList[V]) Items() []V {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.list
}

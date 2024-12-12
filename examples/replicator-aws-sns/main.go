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
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
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

		snsTopic   string
		snsRegion  string
		snsSubject string

		progressTable string
	)

	flag.StringVar(&keyspace, "keyspace", "", "keyspace name")
	flag.StringVar(&table, "table", "", "table name; you can specify multiple table by separating them with a comma")
	flag.StringVar(&source, "source", "", "address of a node in source cluster")
	flag.StringVar(&progressNode, "progress-node", "", "address of a node in progress cluster")
	flag.StringVar(&readConsistency, "read-consistency", "", "consistency level used to read from cdc log (one, quorum, all)")
	flag.StringVar(&writeConsistency, "write-consistency", "", "consistency level used to write to the destination cluster (one, quorum, all)")
	flag.StringVar(&progressTable, "progress-table", "", "fully-qualified name of the table in the destination cluster to use for saving progress; if omitted, the progress won't be saved")

	flag.StringVar(&snsTopic, "sns-topic", "", "SNS Topic ARN")
	flag.StringVar(&snsSubject, "sns-subject", "", "SNS Subject")
	flag.StringVar(&snsRegion, "sns-region", "", "AWS region where SNS topic is deployed")

	flag.String("mode", "", "mode (ignored)")
	flag.Parse()

	clRead := parseConsistency(readConsistency)
	clWrite := parseConsistency(writeConsistency)

	adv := scyllacdc.AdvancedReaderConfig{
		ConfidenceWindowSize:   30 * time.Second,
		ChangeAgeLimit:         10 * time.Minute,
		QueryTimeWindowSize:    60 * time.Second,
		PostEmptyQueryDelay:    30 * time.Second,
		PostNonEmptyQueryDelay: 10 * time.Second,
		PostFailedQueryDelay:   1 * time.Second,
	}

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
		snsTopic, snsSubject, snsRegion,
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
	topic, subject, region string,
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
		subject:  subject,
		region:   region,
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
	rowsRead *int64
	topic    string
	subject  string
	region   string
	logger   scyllacdc.Logger
}

func (rf *replicatorFactory) CreateChangeConsumer(
	ctx context.Context,
	input scyllacdc.CreateChangeConsumerInput,
) (scyllacdc.ChangeConsumer, error) {
	splitTableName := strings.SplitN(input.TableName, ".", 2)
	if len(splitTableName) < 2 {
		return nil, fmt.Errorf("table name is not fully qualified: %s", input.TableName)
	}
	return NewSNSReplicator(ctx, rf.topic, rf.subject, rf.region, rf.rowsRead, input.StreamID, input.ProgressReporter, rf.logger)
}

type SNSReplicator struct {
	snsClient   *sns.Client
	snsTopic    string
	snsSubject  string
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

func NewSNSReplicator(
	ctx context.Context,
	topic, subject, region string,
	count *int64,
	streamID scyllacdc.StreamID,
	reporter *scyllacdc.ProgressReporter,
	logger scyllacdc.Logger,
) (*SNSReplicator, error) {
	var opts [](func(*config.LoadOptions) error)
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	dr := &SNSReplicator{
		snsClient:  sns.NewFromConfig(awsCfg),
		snsTopic:   topic,
		snsSubject: subject,
		totalCount: count,
		streamID:   streamID,
		reporter:   scyllacdc.NewPeriodicProgressReporter(logger, reportPeriod, reporter),
	}

	dr.reporter.Start(ctx)
	return dr, nil
}

func (r *SNSReplicator) Consume(ctx context.Context, c scyllacdc.Change) error {
	timestamp := c.GetCassandraTimestamp()
	if showTimestamps {
		log.Printf("[%s] Processing timestamp: %s (%s)\n", c.StreamID, c.Time, c.Time.Time())
	}

	for _, change := range c.Delta {
		if err := r.sendChangeToSNS(ctx, change, timestamp, "Delta"); err != nil {
			return err
		}
	}

	for _, change := range c.PreImage {
		if err := r.sendChangeToSNS(ctx, change, timestamp, "PreImage"); err != nil {
			return err
		}
	}

	for _, change := range c.PostImage {
		if err := r.sendChangeToSNS(ctx, change, timestamp, "PostImage"); err != nil {
			return err
		}
	}

	r.reporter.Update(c.Time)
	r.localCount += int64(len(c.Delta))

	return nil
}

func (r *SNSReplicator) sendChangeToSNS(ctx context.Context, change *scyllacdc.ChangeRow, timestamp int64, recType string) error {
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

	_, err = r.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(r.snsTopic),
		Message:  aws.String(string(msg)),
		Subject:  aws.String(r.snsSubject),
	})

	if err != nil {
		return fmt.Errorf("failed to send message to SNS: %w", err)
	}
	return nil
}

func (r *SNSReplicator) End() error {
	log.Printf("Streams [%s]: processed %d changes in total", r.streamID, r.localCount)
	atomic.AddInt64(r.totalCount, r.localCount)
	_ = r.reporter.SaveAndStop(context.Background())
	return nil
}

func (r *SNSReplicator) Empty(ctx context.Context, ackTime gocql.UUID) error {
	log.Printf("Streams [%s]: saw no changes up to %s", r.streamID, ackTime.Time())
	r.reporter.Update(ackTime)
	return nil
}

// Make sure that SNSReplicator supports the ChangeOrEmptyNotificationConsumer interface
var _ scyllacdc.ChangeOrEmptyNotificationConsumer = (*SNSReplicator)(nil)

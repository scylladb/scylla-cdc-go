package scyllacdc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var (
	ErrNoGenerationsPresent               = errors.New("there are no generations present")
	ErrNoSupportedGenerationTablesPresent = errors.New("no supported generation tables are present")
)

const (
	generationsTableNamePre4_4 = "system_distributed.cdc_streams_descriptions"

	timestampsTableSince4_4 = "system_distributed.cdc_generation_timestamps"
	streamsTableSince4_4    = "system_distributed.cdc_streams_descriptions_v2"

	timestampsTableForTablets = "system.cdc_timestamps"
	streamsTableForTablets    = "system.cdc_streams"

	getGenerationTimesQueryForTablets = "SELECT timestamp FROM " + timestampsTableForTablets + " WHERE keyspace_name = ? AND table_name = ?"
	getGenerationQueryForTablets      = "SELECT stream_id FROM " + streamsTableForTablets + " WHERE keyspace_name = ? AND table_name = ? AND timestamp = ? AND stream_state = ?"

	// TODO: Switch to a model which reacts to cluster state changes
	// and forces a refresh when all worker goroutines did not report any
	// changes for some time
	generationFetchPeriod time.Duration = 15 * time.Second
)

// follows the stream_state column in system.cdc_streams
const (
	StreamStateCurrent = 0
	StreamStateClosed  = 1
	StreamStateOpened  = 2
)

type generation struct {
	startTime time.Time
	streams   []StreamID
}

// StreamID represents an ID of a stream from a CDC log (cdc$time column).
type StreamID []byte

// String is needed to implement the fmt.Stringer interface.
func (sid StreamID) String() string {
	return hex.EncodeToString(sid)
}

type timeList []time.Time

func (tl timeList) Len() int {
	return len(tl)
}

func (tl timeList) Less(i, j int) bool {
	return tl[i].Before(tl[j])
}

func (tl timeList) Swap(i, j int) {
	tl[i], tl[j] = tl[j], tl[i]
}

type generationFetcher struct {
	session  *gocql.Session
	lastTime time.Time
	logger   Logger

	pushedFirst bool

	generationCh chan *generation
	refreshCh    chan struct{}
	stopCh       chan struct{}

	source generationSource
}

func newGenerationFetcher(
	session *gocql.Session,
	startFrom time.Time,
	logger Logger,
) (*generationFetcher, error) {
	source, err := chooseGenerationSource(session, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to detect version of the generation tables used by the cluster: %v", err)
	}

	gf := &generationFetcher{
		session:  session,
		lastTime: startFrom,
		logger:   logger,

		generationCh: make(chan *generation, 1),
		stopCh:       make(chan struct{}),
		refreshCh:    make(chan struct{}, 1),

		source: source,
	}
	return gf, nil
}

func chooseGenerationSource(session *gocql.Session, logger Logger) (generationSource, error) {
	hasPre4_4, err := isTableInSchema(session, generationsTableNamePre4_4)
	if err != nil {
		return nil, err
	}
	hasPost4_4, err := isTableInSchema(session, streamsTableSince4_4)
	if err != nil {
		return nil, err
	}

	if !hasPost4_4 && !hasPre4_4 {
		// There are no tables we know how to use - return an error
		return nil, ErrNoSupportedGenerationTablesPresent
	}

	if hasPost4_4 && !hasPre4_4 {
		// There is only 4.4+ table, we can immediately start
		// using the new table
		return &generationSourceSince4_4{
			session: session,
			logger:  logger,
		}, nil
	}

	// If we are here, then the pre-4.4 table is there for sure
	// If there is no 4.4+ table - we will start using it right away
	// If there is a 4.4+ table - the maybeUpgrade function
	// will take care of switching to the new table, but only after
	// generation rewriting completes

	return &generationSourcePre4_4{
		session: session,
		logger:  logger,
	}, nil
}

func (gf *generationFetcher) Run(ctx context.Context) error {
	l := gf.logger

	l.Printf("starting generation fetcher loop")

outer:
	for {
		// Generation processing can take some time, so start calculating
		// the next poll time starting from now
		waitC := time.After(generationFetchPeriod)

		gf.tryFetchGenerations()

		select {
		// Give priority to the stop channel and the context
		case <-gf.stopCh:
			break outer
		case <-ctx.Done():
			return ctx.Err()
		default:
			select {
			case <-gf.stopCh:
				break outer
			case <-ctx.Done():
				return ctx.Err()
			case <-waitC:
			case <-gf.refreshCh:
			}
		}
	}

	l.Printf("stopped generation fetcher")
	close(gf.generationCh)
	return nil
}

func (gf *generationFetcher) tryFetchGenerations() {
	// Decide on the consistency to use
	size, err := gf.getClusterSize()
	if err != nil {
		gf.logger.Printf("an error occurred while determining cluster size: %s", err)
		return
	}

	consistency := gocql.One
	if size >= 2 {
		consistency = gocql.Quorum
	}

	// Try switching to a new format before fetching any generations
	newSource, err := gf.source.maybeUpgrade()
	if err != nil {
		gf.logger.Printf("an error occurred while trying to switch to new generations format: %s", err)
	} else {
		gf.source = newSource
	}

	// Fetch some generation times
	times, err := gf.source.getGenerationTimes(consistency)
	if err != nil {
		gf.logger.Printf("an error occurred while fetching generation times: %s", err)
		return
	}
	sort.Sort(timeList(times))

	fetchAndPush := func(t time.Time) (shouldBreak bool) {
		streams, err := gf.source.getGeneration(t, consistency)
		if err != nil {
			gf.logger.Printf("an error occurred while fetching generation streams for %s: %s", t, err)
			return true
		}
		gen := &generation{t, streams}
		if shouldStop := gf.pushGeneration(gen); shouldStop {
			return true
		}
		return false
	}

	var prevTime time.Time

	maybePushFirst := func() bool {
		if !gf.pushedFirst {
			// When we start, we need to push the generation that is being
			// currently open. If we are here, then it means we arrived
			// at the timestamp of the first generation which is after
			// the timestamp from which we wish to start replicating.
			// We need to push the previous generation first.
			// If there was no previous generation, then it probably means
			// that the generation we arrived at is the very first generation
			// in the cluster
			if !prevTime.IsZero() {
				if shouldBreak := fetchAndPush(prevTime); shouldBreak {
					return true
				}
			}
			gf.pushedFirst = true
		}
		return false
	}

	for _, t := range times {
		if gf.lastTime.Before(t) {

			if shouldBreak := maybePushFirst(); shouldBreak {
				return
			}

			if shouldBreak := fetchAndPush(t); shouldBreak {
				return
			}
			gf.lastTime = t
		}
		prevTime = t
	}

	_ = maybePushFirst()
}

func (gf *generationFetcher) Get(ctx context.Context) (*generation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case gen := <-gf.generationCh:
		return gen, nil
	}
}

func (gf *generationFetcher) Stop() {
	close(gf.stopCh)
}

func (gf *generationFetcher) TriggerRefresh() {
	select {
	case gf.refreshCh <- struct{}{}:
	default:
	}
}

func (gf *generationFetcher) pushGeneration(gen *generation) (shouldStop bool) {
	gf.logger.Printf("pushing generation %v", gen.startTime)
	select {
	case <-gf.stopCh:
		return true
	case gf.generationCh <- gen:
		gf.lastTime = gen.startTime
		return false
	}
}

// Unfortunately, gocql does not expose information about the cluster,
// therefore we need to poll system.peers manually
func (gf *generationFetcher) getClusterSize() (int, error) {
	var size int
	err := gf.session.Query("SELECT COUNT(*) FROM system.peers").Scan(&size)
	if err != nil {
		return 0, err
	}
	return size + 1, nil
}

type generationSource interface {
	getGeneration(genTime time.Time, consistency gocql.Consistency) ([]StreamID, error)
	getGenerationTimes(consistency gocql.Consistency) ([]time.Time, error)

	maybeUpgrade() (generationSource, error)
}

type generationSourcePre4_4 struct {
	session *gocql.Session
	logger  Logger
}

func (gs *generationSourcePre4_4) getGeneration(genTime time.Time, consistency gocql.Consistency) ([]StreamID, error) {
	var streams []StreamID
	err := gs.session.Query("SELECT streams FROM "+generationsTableNamePre4_4+" WHERE time = ?", genTime).
		Consistency(consistency).
		Scan(&streams)
	if err != nil {
		return nil, err
	}
	return streams, err
}

func (gs *generationSourcePre4_4) getGenerationTimes(consistency gocql.Consistency) ([]time.Time, error) {
	iter := gs.session.Query("SELECT time FROM " + generationsTableNamePre4_4).
		Consistency(consistency).
		Iter()
	var (
		times    []time.Time
		currTime time.Time
	)
	for iter.Scan(&currTime) {
		times = append(times, currTime)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return times, nil
}

// Follows the migration procedure from Scylla's documentation
// https://docs.scylladb.com/using-scylla/cdc/cdc-querying-streams/
func (gs *generationSourcePre4_4) maybeUpgrade() (generationSource, error) {
	// Check if table is present
	hasNewStreamsTable, err := isTableInSchema(gs.session, streamsTableSince4_4)
	if err != nil {
		return gs, err
	}
	if !hasNewStreamsTable {
		// Don't upgrade, the new table is not there yet
		return gs, nil
	}

	// Was the migration completed?
	data := make(map[string]interface{})
	err = gs.session.Query("SELECT streams_timestamp FROM system.cdc_local WHERE key = 'rewritten'").
		MapScan(data)

	if err == gocql.ErrNotFound {
		// The "rewritten" row is not present yet, this means that the generations
		// weren't rewritten yet
		// Try again later
		return gs, nil
	}

	if err != nil {
		// Some other error
		return gs, err
	}

	newGs := &generationSourceSince4_4{
		session: gs.session,
		logger:  gs.logger,
	}

	return newGs.maybeUpgrade()
}

type generationSourceSince4_4 struct {
	session *gocql.Session
	logger  Logger
}

func (gs *generationSourceSince4_4) getGeneration(genTime time.Time, consistency gocql.Consistency) ([]StreamID, error) {
	var streams []StreamID
	iter := gs.session.Query("SELECT streams FROM "+streamsTableSince4_4+" WHERE time = ?", genTime).
		Consistency(consistency).
		Iter()

	var vnodeStreams []StreamID
	for iter.Scan(&vnodeStreams) {
		streams = append(streams, vnodeStreams...)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}
	return streams, nil
}

func (gs *generationSourceSince4_4) getGenerationTimes(consistency gocql.Consistency) ([]time.Time, error) {
	iter := gs.session.Query("SELECT time FROM " + timestampsTableSince4_4 + " WHERE key = 'timestamps'").
		Consistency(consistency).
		Iter()
	var (
		times    []time.Time
		currTime time.Time
	)
	for iter.Scan(&currTime) {
		times = append(times, currTime)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return times, nil
}

func (gs *generationSourceSince4_4) maybeUpgrade() (generationSource, error) {
	// No newer format is known
	return gs, nil
}

type generationSourceTablets struct {
	session   *gocql.Session
	logger    Logger
	keyspace  string
	tableName string
}

func (gs *generationSourceTablets) getGeneration(genTime time.Time, consistency gocql.Consistency) ([]StreamID, error) {
	var streams []StreamID
	iter := gs.session.Query(getGenerationQueryForTablets, gs.keyspace, gs.tableName, genTime, StreamStateCurrent).
		Consistency(consistency).
		Iter()

	var streamID StreamID
	for iter.Scan(&streamID) {
		streams = append(streams, streamID)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get generations for %s.%s: %w", gs.keyspace, gs.tableName, err)
	}
	return streams, nil
}

func (gs *generationSourceTablets) getGenerationTimes(consistency gocql.Consistency) ([]time.Time, error) {
	iter := gs.session.Query(getGenerationTimesQueryForTablets, gs.keyspace, gs.tableName).
		Consistency(consistency).
		Iter()
	var (
		times    []time.Time
		currTime time.Time
	)
	for iter.Scan(&currTime) {
		times = append(times, currTime)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get timestamps for %s.%s: %w", gs.keyspace, gs.tableName, err)
	}
	return times, nil
}

func (gs *generationSourceTablets) maybeUpgrade() (generationSource, error) {
	// No newer format is known for tablets
	return gs, nil
}

// Takes a fully-qualified name of a table and returns if a table of given name
// is in the schema.
// Panics if the table name is not qualified, i.e. it does not contain a dot.
func isTableInSchema(session *gocql.Session, tableName string) (bool, error) {
	decomposed := strings.SplitN(tableName, ".", 2)
	if len(decomposed) < 2 {
		panic("unqualified table name passed to inTableInSchema")
	}

	keyspace := decomposed[0]
	table := decomposed[1]

	meta, err := session.KeyspaceMetadata(keyspace)
	if err == gocql.ErrKeyspaceDoesNotExist {
		return false, nil
	} else if err != nil {
		return false, err
	}

	_, ok := meta.Tables[table]
	return ok, nil
}

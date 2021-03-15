package scyllacdc

import (
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"time"

	"github.com/gocql/gocql"
)

var (
	ErrNoGenerationsPresent = errors.New("there are no generations present")
)

const (
	generationsTableNamePre4_4 = "system_distributed.cdc_streams_descriptions"

	// TODO: Switch to a model which reacts to cluster state changes
	// and forces a refresh when all worker goroutines did not report any
	// changes for some time
	generationFetchPeriod time.Duration = 15 * time.Second
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
	gf := &generationFetcher{
		session:  session,
		lastTime: startFrom,
		logger:   logger,

		generationCh: make(chan *generation, 1),
		stopCh:       make(chan struct{}),
		refreshCh:    make(chan struct{}, 1),

		source: &generationSourcePre4_4{
			session: session,
			logger:  logger,
		},
	}
	return gf, nil
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
	if size == 2 {
		consistency = gocql.Quorum
	} else if size >= 3 {
		consistency = gocql.All
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
		gf.logger.Printf("an error occured while fetching generation times: %s", err)
		return
	}
	sort.Sort(timeList(times))

	fetchAndPush := func(t time.Time) (shouldBreak bool) {
		streams, err := gf.source.getGeneration(t, consistency)
		if err != nil {
			gf.logger.Printf("an error occured while fetching generation streams for %s: %s", t, err)
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

func (gs *generationSourcePre4_4) maybeUpgrade() (generationSource, error) {
	return gs, nil
}

// Finds a name of a supported table for fetching cdc streams
func getGenerationsTableName(session *gocql.Session) (string, error) {
	return "system_distributed.cdc_streams_descriptions", nil
}

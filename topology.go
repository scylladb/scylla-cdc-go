package scylla_cdc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var (
	// The table that keeps names of the generations changed names.
	// This is a list of known supported names, starting from the newest one.
	generationsTableNames []string = []string{
		"system_distributed.cdc_streams_descriptions", // Introduced in Scylla 4.3
		"system_distributed.cdc_streams",              // Introduced in Scylla 4.1
		"system_distributed.cdc_description",
	}
)

var (
	ErrNoGenerationsTable   = errors.New("cluster does not have a known table to read CDC generations from")
	ErrNoGenerationsPresent = errors.New("there are no generations present")
)

const (
	// TODO: Switch to a model which reacts to cluster state changes
	// and forces a refresh when all worker goroutines did not report any
	// changes for some time
	generationFetchPeriod time.Duration = 15 * time.Second
)

type generation struct {
	startTime time.Time
	streams   []StreamID
}

type StreamID []byte

type generationList []*generation

func (gl generationList) Len() int {
	return len(gl)
}

func (gl generationList) Less(i, j int) bool {
	return gl[i].startTime.Before(gl[j].startTime)
}

func (gl generationList) Swap(i, j int) {
	gl[i], gl[j] = gl[j], gl[i]
}

type generationFetcher struct {
	session        *gocql.Session
	clusterTracker *ClusterStateTracker
	genTableName   string
	lastTime       time.Time
	logger         Logger

	consistency gocql.Consistency

	generationCh chan *generation
	refreshCh    chan struct{}
	stopCh       chan struct{}
}

func newGenerationFetcher(
	session *gocql.Session,
	clusterTracker *ClusterStateTracker,
	startFrom time.Time,
	logger Logger,
) (*generationFetcher, error) {
	tableName, err := getGenerationsTableName(session)
	if err != nil {
		return nil, err
	}

	// Detect consistency, based on the cluster size
	clusterSize := 1
	if clusterTracker != nil {
		clusterSize = clusterTracker.GetClusterSize()
	}

	consistency := gocql.One
	if clusterSize == 2 {
		consistency = gocql.Quorum
	} else if clusterSize > 2 {
		consistency = gocql.All
	}

	gf := &generationFetcher{
		session:        session,
		clusterTracker: clusterTracker,
		genTableName:   tableName,
		lastTime:       startFrom,
		logger:         logger,

		consistency: consistency,

		generationCh: make(chan *generation, 1),
		stopCh:       make(chan struct{}),
		refreshCh:    make(chan struct{}, 1),
	}
	return gf, nil
}

func (gf *generationFetcher) Run(ctx context.Context) error {
	l := gf.logger

	queryGensUpToTime := gf.session.Query(fmt.Sprintf("SELECT time, streams FROM %s WHERE time <= ? ALLOW FILTERING", gf.genTableName))
	queryGensAfterTime := gf.session.Query(fmt.Sprintf("SELECT time, streams FROM %s WHERE time > ? ALLOW FILTERING", gf.genTableName))

	// Find the generation which contains the timestamp from which we should
	// start reading
	gl, err := gf.fetchFromGenerationsTable(queryGensUpToTime, gf.lastTime)
	if err != nil {
		close(gf.generationCh)
		return err
	}

	if len(gl) > 0 {
		first := gl[len(gl)-1]

		gf.logger.Printf("pushing generation %v", first.startTime)
		gf.generationCh <- first
		gf.lastTime = first.startTime
	}

	// Periodically poll for newer generations
	l.Printf("starting generation fetcher loop")
outer:
	for {
		// Generation processing can take some time, so start calculating
		// the next poll time starting from now
		waitC := time.After(generationFetchPeriod)

		// See if there are any new generations
		gl, err := gf.fetchFromGenerationsTable(queryGensAfterTime, gf.lastTime)
		if err != nil {
			l.Printf("an error occurred while trying to fetch new generations: %v", err)
		} else {
			// Push generations that we fetched to generationCh
			for _, g := range gl {
				if shouldStop := gf.pushGeneration(g); shouldStop {
					break
				}
			}
		}

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

func (gf *generationFetcher) fetchFromGenerationsTable(
	query *gocql.Query,
	timePoint time.Time,
) ([]*generation, error) {
	iter := query.Consistency(gf.consistency).Bind(timePoint).Iter()

	var gl generationList
	for {
		gen := &generation{}
		if !iter.Scan(&gen.startTime, &gen.streams) {
			break
		}
		gl = append(gl, gen)
	}
	if err := iter.Close(); err != nil {
		gf.logger.Printf("an error occured while fetching generations: %s", err)
		return nil, err
	}

	if len(gl) > 0 {
		gf.logger.Printf("poll returned %d generations", len(gl))
	}

	sort.Sort(gl)
	return gl, nil
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

// Finds a name of a supported table for fetching cdc streams
func getGenerationsTableName(session *gocql.Session) (string, error) {
	for _, name := range generationsTableNames {
		splitName := strings.Split(name, ".")
		keyspaceName := splitName[0]
		tableName := splitName[1]

		queryString := fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspaceName, tableName)
		iter := session.Query(queryString).Consistency(gocql.One).Iter()
		var count int
		iter.Scan(&count)

		if err := iter.Close(); err != nil {
			return "", err
		}

		if count == 1 {
			return name, nil
		}
	}

	return "", ErrNoGenerationsTable
}

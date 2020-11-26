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
	generationFetchPeriod time.Duration = 15 * time.Second
)

type generation struct {
	startTime time.Time
	streams   []stream
}

type stream []byte

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

	generationCh chan *generation
	refreshCh    chan struct{}
	stopCh       chan struct{}
}

func newGenerationFetcher(
	session *gocql.Session,
	clusterTracker *ClusterStateTracker,
	startFrom time.Time,
) (*generationFetcher, error) {
	tableName, err := getGenerationsTableName(session)
	if err != nil {
		return nil, err
	}

	gf := &generationFetcher{
		session:        session,
		clusterTracker: clusterTracker,
		genTableName:   tableName,
		lastTime:       startFrom,

		generationCh: make(chan *generation),
		stopCh:       make(chan struct{}),
		refreshCh:    make(chan struct{}, 1),
	}
	return gf, nil
}

func (gf *generationFetcher) run(ctx context.Context) error {
	// Detect consistency, based on the cluster size
	clusterSize := 1
	if gf.clusterTracker != nil {
		clusterSize = gf.clusterTracker.GetClusterSize()
	}

	consistency := gocql.One
	if clusterSize == 2 {
		consistency = gocql.Quorum
	} else if clusterSize > 2 {
		consistency = gocql.All
	}

	// Prepare the query
	queryString := fmt.Sprintf("SELECT time, streams FROM %s WHERE time > ? ALLOW FILTERING", gf.genTableName)
	q := gf.session.Query(queryString).Consistency(consistency)

	ticker := time.NewTicker(generationFetchPeriod)

	// We want to be able to reset the ticker by re-creating it
	// Therefore we can't write `defer ticker.Stop()` because it will bind
	// the deferred invocation to the current `ticker, not the one that is
	// assigned to the `ticker` variable
	defer func() {
		ticker.Stop()
	}()

outer:
	for {
		var gl generationList

		// See if there are any new generations
		iter := q.Bind(gf.lastTime).Iter()
		for {
			gen := &generation{}
			if !iter.Scan(&gen.startTime, &gen.streams) {
				break
			}
			gl = append(gl, gen)
		}

		if err := iter.Close(); err != nil {
			return err
		}

		sort.Sort(gl)

		if len(gl) > 0 {
			// Save the timestamp of the oldest generation
			gf.lastTime = gl[len(gl)-1].startTime
		}

		// Push generations that we fetched to generationCh
		for _, g := range gl {
			select {
			case <-gf.stopCh:
				break outer
			case gf.generationCh <- g:
				fmt.Printf("Pushed generation %s\n", g.startTime)
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
			case <-ticker.C:
			case <-gf.refreshCh:
				// TODO: In Go 1.15, we can change it to ticker.Reset()
				// This will also let us get rid of the trick in defer
				ticker.Stop()
				ticker = time.NewTicker(generationFetchPeriod)
			}
		}
	}

	return nil
}

func (gf *generationFetcher) get(ctx context.Context) (*generation, error) {
	select {
	case <-gf.stopCh:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case gen := <-gf.generationCh:
		return gen, nil
	}
}

func (gf *generationFetcher) stop() {
	close(gf.stopCh)
}

func (gf *generationFetcher) triggerRefresh() {
	select {
	case gf.refreshCh <- struct{}{}:
	default:
	}
}

// Finds the most recent generation
func (r *Reader) findLatestGeneration() (*generation, error) {
	tableName, err := getGenerationsTableName(r.config.Session)
	if err != nil {
		return nil, err
	}

	// Choose the most recent generation
	queryString := fmt.Sprintf("SELECT time, expired, streams FROM %s BYPASS CACHE", tableName)

	// Detect consistency, based on the cluster size
	clusterSize := 1
	if r.config.ClusterStateTracker != nil {
		clusterSize = r.config.ClusterStateTracker.GetClusterSize()
	}

	consistency := gocql.One
	if clusterSize == 2 {
		consistency = gocql.Quorum
	} else if clusterSize > 2 {
		consistency = gocql.All
	}

	iter := r.config.Session.Query(queryString).Consistency(consistency).Iter()

	var timestamp, bestTimestamp, expired time.Time
	var streams, bestStreams []stream

	for iter.Scan(&timestamp, &expired, &streams) {
		if bestTimestamp.Before(timestamp) {
			bestTimestamp = timestamp
			bestStreams = streams
		}
	}

	if err = iter.Close(); err != nil {
		return nil, err
	}

	if len(bestStreams) == 0 {
		return nil, ErrNoGenerationsPresent
	}

	gen := &generation{
		startTime: bestTimestamp,
		streams:   bestStreams,
	}

	return gen, nil
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

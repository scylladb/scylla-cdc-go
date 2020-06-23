package scylla_cdc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

var (
	// The table that keeps names of the generations changed names.
	// This is a list of known supported names, starting from the newest one.
	generationsTableNames []string = []string{
		"system_distributed.cdc_streams", // Introduced in Scylla 4.1
		"system_distributed.cdc_description",
	}
)

var (
	ErrNoGenerationsTable   = errors.New("cluster does not have a known table to read CDC generations from")
	ErrNoGenerationsPresent = errors.New("there are no generations present")
)

type generation struct {
	startTime time.Time
	streams   []stream
}

type stream []byte

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

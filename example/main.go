package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/gocql/gocql"
	scylla_cdc "github.com/piodul/scylla-cdc-go"
)

// TODO: Remove?

func main() {
	// This wrapper around HostSelectionPolicy is used in order to forward information about the cluster topology
	// to the Reader. This is a limitation of gocql library and the need for it will be removed if the needed
	// functionality is implemented in gocql.
	tracker := scylla_cdc.NewClusterStateTracker(gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()))

	// Configure a session first
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = tracker
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Configuration for the CDC reader
	cfg := &scylla_cdc.ReaderConfig{
		Session:               session,
		Consistency:           gocql.One,
		TableNames:            []string{"ks.tbl"},
		ChangeConsumerFactory: scylla_cdc.MakeChangeConsumerFactoryFromFunc(simpleConsumer),
		ClusterStateTracker:   tracker,
		ProgressManager:       &scylla_cdc.NoProgressManager{},

		Logger: log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile),
	}

	reader, err := scylla_cdc.NewReader(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// React to Ctrl+C signal, and stop gracefully after the first signal
	// Second signal exits the process
	signalC := make(chan os.Signal)
	go func() {
		<-signalC
		reader.Stop()

		<-signalC
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	if err := reader.Run(context.Background()); err != nil {
		log.Fatalf("replicator failed: %s", err)
	}
}

func simpleConsumer(tableName string, change scylla_cdc.Change) error {
	fmt.Println(tableName, change)
	return nil
}

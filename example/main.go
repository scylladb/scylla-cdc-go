package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gocql/gocql"
	scylla_cdc "github.com/piodul/scylla-cdc-go"
)

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
		Session:             session,
		Consistency:         gocql.One,
		Context:             context.Background(),
		LogTableName:        "ks.tbl_scylla_cdc_log",
		ChangeConsumer:      scylla_cdc.ChangeConsumerFunc(simpleConsumer),
		ClusterStateTracker: tracker,

		// Those two are the main knobs that control library's polling strategy.
		// Refer to the README for information on how to configure them.
		//
		// This configuration represents the first strategy (quick processing, with duplicates).
		LowerBoundReadOffset: 3 * time.Second,
		UpperBoundReadOffset: 0,
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

	if err := reader.Run(); err != nil {
		log.Fatal(err)
	}
}

func simpleConsumer(change scylla_cdc.Change) {
	fmt.Printf("Current time is %v\n", time.Now())
	fmt.Printf("Consuming change of type %d\n", change.GetOperation())
	fmt.Printf("Time is %v\n", change.GetTime().Time())
	fmt.Printf("TTL is %d\n", change.GetTTL())
	fmt.Printf("Value of column \"pk\" is %v\n", change.GetValue("pk"))
	fmt.Printf("Value of column \"c\" is %v\n", change.GetValue("c"))
	fmt.Printf("Was \"c\" column deleted? %t\n", change.IsDeleted("c"))
	fmt.Printf("Deleted elements of column \"c\": %v\n", change.GetDeletedElements("c"))
	fmt.Println("")
}

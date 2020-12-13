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
	// Configure a session first
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/gocql/gocql"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

// Make sure you create the following table before you run this example:
// CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': 'true'};

func main() {
	var (
		keyspace string
		table    string
		source   string
	)

	flag.StringVar(&keyspace, "keyspace", "ks", "keyspace name")
	flag.StringVar(&table, "table", "tbl", "table name")
	flag.StringVar(&source, "source", "127.0.0.1", "address of a node in the cluster")
	flag.Parse()

	if err := run(context.Background(), source, keyspace, table); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, source, keyspace, table string) error {
	cluster := gocql.NewCluster(source)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	cfg := &scyllacdc.ReaderConfig{
		Session:               session,
		TableNames:            []string{keyspace + "." + table},
		ChangeConsumerFactory: changeConsumerFactory,
		Logger:                log.New(os.Stderr, "", log.Ldate|log.Lshortfile),
	}

	reader, err := scyllacdc.NewReader(ctx, cfg)
	if err != nil {
		return err
	}

	return reader.Run(ctx)
}

func consumeChange(ctx context.Context, tableName string, c scyllacdc.Change) error {
	for _, changeRow := range c.Delta {
		pkRaw, _ := changeRow.GetValue("pk")
		ckRaw, _ := changeRow.GetValue("ck")
		v := changeRow.GetAtomicChange("v")

		pk := pkRaw.(*int)
		ck := ckRaw.(*int)

		fmt.Printf("Operation: %s, pk: %s, ck: %s\n", changeRow.GetOperation(),
			nullableIntToStr(pk), nullableIntToStr(ck))

		if v.IsDeleted {
			fmt.Printf("  Column v was set to null/deleted\n")
		} else {
			vInt := v.Value.(*int)
			if vInt != nil {
				fmt.Printf("  Column v was set to %d\n", *vInt)
			} else {
				fmt.Print("  Column v was not changed\n")
			}
		}
	}

	return nil
}

func nullableIntToStr(i *int) string {
	if i == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *i)
}

var changeConsumerFactory = scyllacdc.MakeChangeConsumerFactoryFromFunc(consumeChange)

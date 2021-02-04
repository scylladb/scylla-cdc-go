package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/gocql/gocql"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

// Make sure you create the following table before you run this example:
// CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': 'true'};

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("local-dc"))
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	cfg := &scyllacdc.ReaderConfig{
		Session:               session,
		TableNames:            []string{"ks.tbl"},
		ChangeConsumerFactory: changeConsumerFactory,
		Logger:                log.New(os.Stderr, "", log.Ldate|log.Lshortfile),
	}

	reader, err := scyllacdc.NewReader(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := reader.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
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

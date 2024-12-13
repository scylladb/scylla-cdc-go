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
	if err := run(context.Background(), []string{"127.0.0.1"}, "local-dc", "ks.tbl"); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, hosts []string, localDC, tableName string) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(localDC))
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	cfg := &scyllacdc.ReaderConfig{
		Session:               session,
		TableNames:            []string{tableName},
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

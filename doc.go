/*
Package scyllacdc is a library that helps develop applications that react
to changes from Scylla's CDC.

It is recommended to get familiar with the Scylla CDC documentation first
in order to understand the concepts used in the documentation of scyllacdc:
https://docs.scylladb.com/using-scylla/cdc/

Overview

The library hides the complexity of reading from CDC log stemming from
the need for polling for changes and handling topology changes. It reads
changes from CDC logs of selected tables and propagates them to instances
of ChangeConsumer - which is an interface that is meant to be implemented
by the user.

Getting started

To start working with the library, you first need to implement your own
logic for consuming changes. The simplest way to do it is to define
a ChangeConsumerFunc which will be called for each change from the CDC log.
For example:

	func printerConsumer(ctx context.Context, tableName string, c scyllacdc.Change) error {
		fmt.Printf("[%s] %#v\n", tableName, c)
	}

For any use case more complicated than above, you will need to define
a ChangeConsumer and a ChangeConsumerFactory:

	type myConsumer struct {
		id        int
		tableName string
	}

	func (mc *myConsumer) Consume(ctx context.Context, change scyllacdc.Change) error {
		fmt.Printf("[%d] [%s] %#v\n", mc.id, mc.tableName, change)
		return nil
	}

	func (mc *myConsumer) End() error {
		return nil
	}

	type myFactory struct {
		nextID int
	}

	func (f *myFactory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
		f.nextID++
		return &myConsumer{
			id:        f.nextID-1,
			tableName: input.TableName,
		}, nil
	}

Next, you need to create and run a scyllacdc.Reader object:

	func main() {
		cluster := gocql.NewCluster("127.0.0.1")
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
		session, err := cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()

		cfg := &scyllacdc.ReaderConfig{
			Session:               session,
			TableNames:            []string{"my_keyspace.my_table"},
			ChangeConsumerFactory: scyllacdc.MakeChangeConsumerFactoryFromFunc(printerConsumer),
			// The above can be changed to:
			// ChangeConsumerFactory: &myFactory{},
		}

		reader, err := scyllacdc.NewReader(context.Background(), cfg)
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
			log.Fatal(err)
		}
	}

Saving progress

The library supports saving progress and restoring from the last saved position.
To enable it, you need to do two things:

First, you need to modify your consumer to regularly save progress. The consumer
receives a *scyllacdc.ProgressReporter object which can be used to save progress
at any point in the lifetime of the consumer.

The library itself doesn't regularly save progress - it only does it by itself
when switching to the next CDC generation. Therefore, the consumer is
responsible for saving the progress regularly.

Example:

	type myConsumer struct {
		// PeriodicProgressReporter is a wrapper around ProgressReporter
		// which rate-limits saving the progress
		reporter *scyllacdc.PeriodicProgressReporter
	}

	func (mc *myConsumer) Consume(ctx context.Context, change scyllacdc.Change) error {
		// ... do work ...

		mc.reporter.Update(change.Time)
		return nil
	}

	func (mc *myConsumer) End() error {
		_ = mc.reporter.SaveAndStop(context.Background())
		return nil
	}

	type myFactory struct {
		session *gocql.Session
	}

	func (f *myFactory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (ChangeConsumer, error)
		reporter := scyllacdc.NewPeriodicProgressReporter(f.session, time.Minute, input.ProgressReporter)
		reporter.Start(ctx)
		return &myConsumer{reporter: reporter}, nil
	}

Then, you need to specify an appropriate ProgressManager in the configuration.
ProgressManager represents a mechanism of saving and restoring progress. You can
use the provided implementations (TableBackedProgressManager), or implement
it yourself.

In the main function:

	cfg.ProgressReporter = scyllacdc.NewTableBackedProgressManager("my_keyspace.progress_table", "my_application_name")

Processing changes

Data from the CDC log is supplied to the ChangeConsumer through Change objects,
which can contain multiple ChangeRow objects. A single ChangeRow corresponds
to a single, full (all columns included) row from the CDC log.

	func (mc *myConsumer) Consume(ctx context.Background, change scyllacdc.Change) error {
		for _, changeRow := range change.Deltas {
			// You can access CDC columns directly via
			// GetValue, IsDeleted, GetDeletedElements
			rawValue, _ := changeRow.GetValue("col_int")
			intValue := rawValue.(*int)
			isDeleted, _ := changeRow.IsDeleted("col_int")
			if isDeleted {
				fmt.Println("Column col_int was set to null")
			} else if intValue != nil {
				fmt.Printf("Column col_int was set to %d\n", *intValue)
			}

			// You can also use convenience functions:
			// GetAtomicChange, GetListChange, GetUDTChange, etc.
			atomicChange := changeRow.GetAtomicChange("col_text")
			strValue := atomicChange.Value.(*string)
			if atomicChange.IsDeleted {
				fmt.Println("Column col_text was deleted")
			} else if strValue != nil {
				fmt.Printf("Column col_text was set to %s\n", *strValue)
			}
		}

		return nil
	}

*/
package scyllacdc

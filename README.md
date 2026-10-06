# scylla-cdc-go

Package scyllacdc is a library that helps develop applications that react
to changes from Scylla's CDC.

It is recommended to get familiar with the Scylla CDC documentation first
in order to understand the concepts used in the documentation of scyllacdc:
https://docs.scylladb.com/using-scylla/cdc/

## Documentation

For an explanation how to use the library, please look at the [godoc documenation](https://godoc.org/github.com/scylladb/scylla-cdc-go).

This repository also includes two [example programs](examples).

## Choosing the confidence window

See [AdvancedReaderConfig.ConfidenceWindowSize](https://pkg.go.dev/github.com/scylladb/scylla-cdc-go#AdvancedReaderConfig.ConfidenceWindowSize)
for how the reader uses this window and how to size it. Before shortening it,
account for past-dated explicit write timestamps as well as the base-table
write timeout and clock skew. The timeout helps bound visibility only when
write and reader consistency levels overlap in the reader's DC (W + R > RF);
otherwise late replica writes can remain invisible beyond the timeout.

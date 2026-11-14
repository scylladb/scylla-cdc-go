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

## Replicator consistency

The `replicator`, `replicator-aws-sns`, and `replicator-gcp-pub` examples default
`--read-consistency` and `--write-consistency` to `local_quorum`. They also accept
`one`, `quorum`, and `all`, case-insensitively. An empty or unsupported value uses
`local_quorum`; unsupported values produce a warning. To retain the previous
replicator defaults, explicitly pass `--read-consistency=one` and
`--write-consistency=one`.

In the ordinary replicator, `--write-consistency` controls destination data
writes. In the SNS and Pub/Sub examples, it controls reads and writes of the CQL
progress table; it does not configure the cloud service. The ordinary replicator's
progress table inherits its destination session's consistency instead.

`LOCAL_QUORUM` refers to the coordinator's datacenter. These example programs do
not pin source, destination, or progress sessions to a datacenter. Before using
them with multiple datacenters, configure each session's host-selection policy
for the intended datacenter and disable cross-datacenter failover, for example:

```go
cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
    gocql.DCAwareRoundRobinPolicy(localDC),
    gocql.HostPolicyOptionDisableDCFailover,
)
```

Choose `localDC` separately for each cluster from the deployment configuration,
and ensure the queried keyspaces have replicas there. Reads and writes that
require overlapping local quorums must use the same datacenter. Disabling
cross-datacenter failover trades availability during a local outage for a fixed
local consistency scope. Explicit `quorum` uses a cross-datacenter quorum when
that behavior is required. A driver update that changes session defaults also
affects inherited progress-table and direct metadata queries.

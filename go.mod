module github.com/scylladb/scylla-cdc-go

go 1.14

require (
	github.com/gocql/gocql v0.0.0-20201215165327-e49edf966d90
	golang.org/x/sync v0.8.0
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.7.3

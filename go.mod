module github.com/scylladb/scylla-cdc-go

go 1.14

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.4.3

require (
	github.com/gocql/gocql v0.0.0-20201215165327-e49edf966d90
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
)

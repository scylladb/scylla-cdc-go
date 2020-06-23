module github.com/piodul/scylla-cdc-go

go 1.14

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.3.4

require (
	github.com/gocql/gocql v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
)

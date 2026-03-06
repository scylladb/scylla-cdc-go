module github.com/scylladb/scylla-cdc-go

go 1.25.0

require (
	github.com/gocql/gocql v1.17.0
	golang.org/x/sync v0.19.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.17.3

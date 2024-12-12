module github.com/scylladb/replicator-was-sns

go 1.22

require (
	github.com/aws/aws-sdk-go-v2 v1.32.6
	github.com/aws/aws-sdk-go-v2/service/sns v1.33.7
	github.com/aws/aws-sdk-go-v2/service/sqs v1.37.2
	github.com/gocql/gocql v0.0.0-20201215165327-e49edf966d90
	github.com/scylladb/scylla-cdc-go v0.0.0-20201215165327-e49edf966d90
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.25 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	golang.org/x/sync v0.8.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.14.4
	github.com/scylladb/scylla-cdc-go => ../../
)

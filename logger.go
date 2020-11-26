package scylla_cdc

type Logger interface {
	Printf(format string, v ...interface{})
}

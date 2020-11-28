package scylla_cdc

type Logger interface {
	Printf(format string, v ...interface{})
}

type noLogger struct{}
func (noLogger) Printf(format string, v ...interface{}) {}

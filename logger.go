package scyllacdc

type Logger interface {
	Printf(format string, v ...interface{})
}

type noLogger struct{}

func (noLogger) Printf(_ string, _ ...interface{}) {}

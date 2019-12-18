package sqsworker

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

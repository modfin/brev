package smtpx

type Logger interface {
	Logf(format string, args ...interface{})
}

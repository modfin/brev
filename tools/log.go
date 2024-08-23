package tools

import (
	"github.com/modfin/henry/mapz"
	"github.com/sirupsen/logrus"
)

func LoggerCloner(l *logrus.Logger) *Logger {
	return &Logger{
		def: l,
	}
}

type Logger struct {
	def *logrus.Logger
}

func (l *Logger) New(name string) *logrus.Logger {

	hooks := mapz.Clone(l.def.Hooks)

	ll := &logrus.Logger{
		Out:          l.def.Out,
		Formatter:    l.def.Formatter,
		Hooks:        hooks,
		Level:        l.def.Level,
		ExitFunc:     l.def.ExitFunc,
		ReportCaller: l.def.ReportCaller,
	}

	ll.AddHook(LoggerWho{Name: name})
	return ll

}

type LoggerWho struct {
	Name string
}

func (w LoggerWho) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (w LoggerWho) Fire(entry *logrus.Entry) error {
	entry.Data["who"] = w.Name
	return nil
}

package cx

import (
	"fmt"
	"log"
)

type Logger interface {
	Log(message interface{})
	Logf(format string, v ...interface{})
}

type defaultLogger struct{}

func NewDefaultLogger() Logger {
	d := &defaultLogger{}
	return d
}

func (d defaultLogger) Log(message interface{}) {
	log.Printf("[CLICKHOUSE BUFFER] %s \n", message)
}

func (d *defaultLogger) Logf(message string, v ...interface{}) {
	d.Log(fmt.Sprintf(message, v...))
}

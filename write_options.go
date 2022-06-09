package clickhousebuffer

import (
	"github.com/zikwall/clickhouse-buffer/v3/src/cx"
	"github.com/zikwall/clickhouse-buffer/v3/src/retry"
)

// Options holds write configuration properties
type Options struct {
	// Maximum number of rows sent to server in single request. Default 5000
	batchSize uint
	// Interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size) . Default 1000ms
	flushInterval uint
	// Debug mode
	isDebug bool
	// Retry is enabled
	isRetryEnabled bool
	// Logger with
	logger cx.Logger
	// Queueable with
	queue retry.Queueable
}

// BatchSize returns size of batch
func (o *Options) BatchSize() uint {
	return o.batchSize
}

// SetBatchSize sets number of rows sent in single request
func (o *Options) SetBatchSize(batchSize uint) *Options {
	o.batchSize = batchSize
	return o
}

// FlushInterval returns flush interval in ms
func (o *Options) FlushInterval() uint {
	return o.flushInterval
}

// SetFlushInterval sets flush interval in ms in which is buffer flushed if it has not been already written
func (o *Options) SetFlushInterval(flushIntervalMs uint) *Options {
	o.flushInterval = flushIntervalMs
	return o
}

func (o *Options) SetDebugMode(isDebug bool) *Options {
	o.isDebug = isDebug
	return o
}

func (o *Options) SetRetryIsEnabled(enabled bool) *Options {
	o.isRetryEnabled = enabled
	return o
}

func (o *Options) SetLogger(logger cx.Logger) *Options {
	o.logger = logger
	return o
}

func (o *Options) SetQueueEngine(queue retry.Queueable) *Options {
	o.queue = queue
	return o
}

// DefaultOptions returns Options object with default values
func DefaultOptions() *Options {
	return &Options{
		batchSize:     5000,
		flushInterval: 1000,
	}
}

package clickhousebuffer

import (
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
	"github.com/zikwall/clickhouse-buffer/v4/src/retry"
)

// Options holds write configuration properties
type Options struct {
	// Maximum number of rows sent to server in single request. Default 5000
	batchSize uint
	// Interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size).
	// Default 1000ms
	flushInterval uint
	// Debug mode
	isDebug bool
	// retry.Retry is enabled
	isRetryEnabled bool
	// cx.Logger with
	logger cx.Logger
	// retry.Queueable with
	queue retry.Queueable
}

// BatchSize returns size of batch
func (o *Options) BatchSize() uint {
	return o.batchSize
}

// SetBatchSize sets number of rows sent in single request, it would be a good practice to remove this function,
// but it is convenient for testing. DO NOT USE in parallel environments
func (o *Options) SetBatchSize(batchSize uint) *Options {
	o.batchSize = batchSize
	return o
}

// FlushInterval returns flush interval in ms
func (o *Options) FlushInterval() uint {
	return o.flushInterval
}

// SetFlushInterval sets flush interval in ms in which is buffer flushed if it has not been already written
// it would be a good practice to remove this function,
// but it is convenient for testing. DO NOT USE in parallel environments
func (o *Options) SetFlushInterval(flushIntervalMs uint) *Options {
	o.flushInterval = flushIntervalMs
	return o
}

// for multithreading systems, you can implement something like this:
//
// func (o *Options) ConcurrentlySetFlushInterval(flushIntervalMs uint) *Options {
//   o.mx.Lock()
//	 o.flushInterval = flushIntervalMs
//   o.mx.Unlock()
//	 return o
// }

// SetDebugMode set debug mode, for logs and errors
//
// Deprecated: use WithDebugMode function with NewOptions
func (o *Options) SetDebugMode(isDebug bool) *Options {
	o.isDebug = isDebug
	return o
}

// SetRetryIsEnabled enable/disable resending undelivered messages
//
// Deprecated: use WithRetry function with NewOptions
func (o *Options) SetRetryIsEnabled(enabled bool) *Options {
	o.isRetryEnabled = enabled
	return o
}

// SetLogger installs a custom implementation of the cx.Logger interface
//
// Deprecated: use WithLogger function with NewOptions
func (o *Options) SetLogger(logger cx.Logger) *Options {
	o.logger = logger
	return o
}

// SetQueueEngine installs a custom implementation of the retry.Queueable interface
//
// Deprecated: use WithRetryQueueEngine function with NewOptions
func (o *Options) SetQueueEngine(queue retry.Queueable) *Options {
	o.queue = queue
	return o
}

func WithBatchSize(size uint) Option {
	return func(o *Options) {
		o.batchSize = size
	}
}

func WithFlushInterval(interval uint) Option {
	return func(o *Options) {
		o.flushInterval = interval
	}
}

func WithDebugMode(isDebug bool) Option {
	return func(o *Options) {
		o.isDebug = isDebug
	}
}

func WithRetry(enabled bool) Option {
	return func(o *Options) {
		o.isRetryEnabled = enabled
	}
}

func WithLogger(logger cx.Logger) Option {
	return func(o *Options) {
		o.logger = logger
	}
}

func WithRetryQueueEngine(queue retry.Queueable) Option {
	return func(o *Options) {
		o.queue = queue
	}
}

type Option func(o *Options)

// NewOptions returns Options object with the ability to set your own parameters
func NewOptions(options ...Option) *Options {
	o := &Options{
		batchSize:     2000,
		flushInterval: 2000,
	}
	for _, option := range options {
		option(o)
	}
	return o
}

// DefaultOptions returns Options object with default values
//
// Deprecated: use NewOptions function with Option callbacks
func DefaultOptions() *Options {
	return &Options{
		batchSize:     5000,
		flushInterval: 1000,
	}
}

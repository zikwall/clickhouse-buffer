package api

// Options holds write configuration properties
type Options struct {
	// Maximum number of rows sent to server in single request. Default 5000
	batchSize uint
	// Interval, in ms, in which is buffer flushed if it has not been already written (by reaching batch size) . Default 1000ms
	flushInterval uint
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

// DefaultOptions returns Options object with default values
func DefaultOptions() *Options {
	return &Options{
		batchSize:     5000,
		flushInterval: 1000,
	}
}

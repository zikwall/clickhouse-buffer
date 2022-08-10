package clickhousebuffer

import (
	"context"
	"sync"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
	"github.com/zikwall/clickhouse-buffer/v4/src/retry"
)

// Client main interface, provides a top-level API.
// Client generates child Writer-s and stores all necessary configuration in itself.
// Client owns an instance of a Clickhouse database connection.
// Client provides a retry.Retryable interface for re-processing packets.
type Client interface {
	// Options returns the options associated with client
	Options() *Options
	// WriteBatch method of sending data to Clickhouse is used implicitly in a non - blocking record,
	// and explicitly in a blocking record
	WriteBatch(context.Context, cx.View, *cx.Batch) error
	// Writer returns the asynchronous, non-blocking, Writer client.
	// Ensures using a single Writer instance for each table pair.
	Writer(context.Context, cx.View, cx.Buffer) Writer
	// WriterBlocking returns the synchronous, blocking, WriterBlocking client.
	// Ensures using a single WriterBlocking instance for each table pair.
	WriterBlocking(cx.View) WriterBlocking
	// RetryClient Get retry client
	RetryClient() retry.Retryable
	// Close ensures all ongoing asynchronous write clients finish.
	Close()
}

// Implementation of the Client interface
type clientImpl struct {
	context       context.Context
	clickhouse    cx.Clickhouse
	options       *Options
	writeAPIs     map[string]Writer
	syncWriteAPIs map[string]WriterBlocking
	mu            sync.RWMutex
	retry         retry.Retryable
	logger        cx.Logger
}

// NewClient creates an object implementing the Client interface with default options
func NewClient(ctx context.Context, clickhouse cx.Clickhouse) Client {
	return NewClientWithOptions(ctx, clickhouse, DefaultOptions())
}

// NewClientWithOptions similar to NewClient except that there is a configuration option
// with an encapsulated setting inside.
// NewClientWithOptions returns implementation of the Client interface.
func NewClientWithOptions(ctx context.Context, clickhouse cx.Clickhouse, options *Options) Client {
	if options.logger == nil {
		options.logger = cx.NewDefaultLogger()
	}
	client := &clientImpl{
		context:       ctx,
		clickhouse:    clickhouse,
		options:       options,
		writeAPIs:     map[string]Writer{},
		syncWriteAPIs: map[string]WriterBlocking{},
		logger:        options.logger,
	}
	// if resending undelivered messages is enabled, safely check all the necessary settings
	if options.isRetryEnabled {
		// if no custom engine is specified for queues, we use the default engine,
		// in most cases, this covers all cases.
		if options.queue == nil {
			options.queue = retry.NewImMemoryQueueEngine()
		}
		client.retry = retry.NewRetry(
			ctx, options.queue, retry.NewDefaultWriter(clickhouse), options.logger, options.isDebug,
		)
	}
	return client
}

// Options return global options object
func (c *clientImpl) Options() *Options {
	return c.options
}

// Writer returns the asynchronous, non-blocking, Writer client.
// Ensures using a single Writer instance for each table pair.
func (c *clientImpl) Writer(ctx context.Context, view cx.View, buf cx.Buffer) Writer {
	key := view.Name
	c.mu.Lock()
	if _, ok := c.writeAPIs[key]; !ok {
		c.writeAPIs[key] = NewWriter(ctx, c, view, buf)
	}
	writer := c.writeAPIs[key]
	c.mu.Unlock()
	return writer
}

// WriterBlocking returns the synchronous, blocking, WriterBlocking client.
// Ensures using a single WriterBlocking instance for each table pair.
func (c *clientImpl) WriterBlocking(view cx.View) WriterBlocking {
	key := view.Name
	c.mu.Lock()
	if _, ok := c.syncWriteAPIs[key]; !ok {
		c.syncWriteAPIs[key] = NewWriterBlocking(c, view)
	}
	writer := c.syncWriteAPIs[key]
	c.mu.Unlock()
	return writer
}

// Close API top-level method safely closes all child asynchronous and synchronous Writer-s
func (c *clientImpl) Close() {
	if c.options.isDebug {
		c.logger.Log("close clickhouse buffer client")
		c.logger.Log("close async writers")
	}
	// closing and destroying all asynchronous writers
	c.mu.Lock()
	for key, w := range c.writeAPIs {
		w.Close()
		delete(c.writeAPIs, key)
	}
	c.mu.Unlock()
	// closing and destroying all synchronous writers
	if c.options.isDebug {
		c.logger.Log("close sync writers")
	}
	c.mu.Lock()
	for key := range c.syncWriteAPIs {
		delete(c.syncWriteAPIs, key)
	}
	c.mu.Unlock()
}

// WriteBatch API top-level method for writing to Clickhouse database.
// All child Writer-s use this method to write their accumulated and encapsulated data.
func (c *clientImpl) WriteBatch(ctx context.Context, view cx.View, batch *cx.Batch) error {
	_, err := c.clickhouse.Insert(ctx, view, batch.Rows())
	if err != nil {
		// if there is an acceptable error and if the functionality of resending data is activated,
		// try to repeat the operation
		if c.options.isRetryEnabled && cx.IsResendAvailable(err) {
			c.retry.Retry(retry.NewPacket(view, batch))
		}
		return err
	}
	return nil
}

// RetryClient returns implementation of the retry.Retryable interface
func (c *clientImpl) RetryClient() retry.Retryable {
	return c.retry
}

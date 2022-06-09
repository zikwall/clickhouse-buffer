package cx

import (
	"context"
	"sync"

	"github.com/zikwall/clickhouse-buffer/v2/src/support"
)

type Client interface {
	// Options returns the options associated with client
	Options() *Options
	// WriteBatch method of sending data to Clickhouse is used implicitly in a non - blocking record,
	// and explicitly in a blocking record
	WriteBatch(context.Context, View, *Batch) error
	// Writer returns the asynchronous, non-blocking, Writer client.
	// Ensures using a single Writer instance for each table pair.
	Writer(View, Buffer) Writer
	// WriterBlocking returns the synchronous, blocking, WriterBlocking client.
	// Ensures using a single WriterBlocking instance for each table pair.
	WriterBlocking(View) WriterBlocking
	// RetryClient Get retry client
	RetryClient() Retryable
	// Close ensures all ongoing asynchronous write clients finish.
	Close()
}

type clientImpl struct {
	context       context.Context
	clickhouse    Clickhouse
	options       *Options
	writeAPIs     map[string]Writer
	syncWriteAPIs map[string]WriterBlocking
	mu            sync.RWMutex
	retry         Retryable
	logger        Logger
}

func NewClient(ctx context.Context, clickhouse Clickhouse) Client {
	return NewClientWithOptions(ctx, clickhouse, DefaultOptions())
}

func NewClientWithOptions(ctx context.Context, clickhouse Clickhouse, options *Options) Client {
	if options.logger == nil {
		options.logger = NewDefaultLogger()
	}
	client := &clientImpl{
		context:       ctx,
		clickhouse:    clickhouse,
		options:       options,
		writeAPIs:     map[string]Writer{},
		syncWriteAPIs: map[string]WriterBlocking{},
		logger:        options.logger,
	}
	if options.isRetryEnabled {
		if options.queue == nil {
			options.queue = NewImMemoryQueueEngine()
		}
		client.retry = NewRetry(
			ctx, options.queue, NewDefaultWriter(clickhouse), options.logger, options.isDebug,
		)
	}
	return client
}

func (c *clientImpl) Options() *Options {
	return c.options
}

func (c *clientImpl) Writer(view View, buf Buffer) Writer {
	key := view.Name
	c.mu.Lock()
	if _, ok := c.writeAPIs[key]; !ok {
		c.writeAPIs[key] = NewWriter(c.context, c, view, buf)
	}
	writer := c.writeAPIs[key]
	c.mu.Unlock()
	return writer
}

func (c *clientImpl) WriterBlocking(view View) WriterBlocking {
	key := view.Name
	c.mu.Lock()
	if _, ok := c.syncWriteAPIs[key]; !ok {
		c.syncWriteAPIs[key] = NewWriterBlocking(c, view)
	}
	writer := c.syncWriteAPIs[key]
	c.mu.Unlock()
	return writer
}

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

func (c *clientImpl) WriteBatch(ctx context.Context, view View, batch *Batch) error {
	_, err := c.clickhouse.Insert(ctx, view, batch.Rows())
	if err != nil {
		// if there is an acceptable error and if the functionality of resending data is activated,
		// try to repeat the operation
		if c.options.isRetryEnabled && support.IsResendAvailable(err) {
			c.retry.Retry(NewRetryPacket(view, batch))
		}
		return err
	}
	return nil
}

func (c *clientImpl) RetryClient() Retryable {
	return c.retry
}

package clickhousebuffer

import (
	"context"
	"sync"

	"github.com/zikwall/clickhouse-buffer/database"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type clientImpl struct {
	context       context.Context
	clickhouse    database.Clickhouse
	options       *Options
	writeAPIs     map[string]Writer
	syncWriteAPIs map[string]WriterBlocking
	mu            sync.RWMutex
	retry         Retryable
	logger        Logger
}

func NewClient(ctx context.Context, clickhouse database.Clickhouse) Client {
	return NewClientWithOptions(ctx, clickhouse, DefaultOptions())
}

func NewClientWithOptions(ctx context.Context, clickhouse database.Clickhouse, options *Options) Client {
	if options.logger == nil {
		options.logger = newDefaultLogger()
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
			options.queue = newImMemoryQueueEngine()
		}
		client.retry = NewRetry(
			ctx, options.queue, NewDefaultWriter(clickhouse), options.logger, options.isDebug,
		)
	}
	return client
}

func (cs *clientImpl) Options() *Options {
	return cs.options
}

func (cs *clientImpl) Writer(view database.View, buf buffer.Buffer) Writer {
	key := view.Name
	cs.mu.Lock()
	if _, ok := cs.writeAPIs[key]; !ok {
		cs.writeAPIs[key] = NewWriter(cs, view, buf, cs.options)
	}
	writer := cs.writeAPIs[key]
	cs.mu.Unlock()
	return writer
}

func (cs *clientImpl) WriterBlocking(view database.View) WriterBlocking {
	key := view.Name
	cs.mu.Lock()
	if _, ok := cs.syncWriteAPIs[key]; !ok {
		cs.syncWriteAPIs[key] = NewWriterBlocking(cs, view)
	}
	writer := cs.syncWriteAPIs[key]
	cs.mu.Unlock()
	return writer
}

func (cs *clientImpl) Close() {
	if cs.options.isDebug {
		cs.logger.Log("close clickhouse buffer client")
		cs.logger.Log("close async writers")
	}
	// Closing and destroying all asynchronous writers
	cs.mu.Lock()
	for key, w := range cs.writeAPIs {
		w.Close()
		delete(cs.writeAPIs, key)
	}
	cs.mu.Unlock()
	// Closing and destroying all synchronous writers
	if cs.options.isDebug {
		cs.logger.Log("close sync writers")
	}
	cs.mu.Lock()
	for key := range cs.syncWriteAPIs {
		delete(cs.syncWriteAPIs, key)
	}
	cs.mu.Unlock()
}

func (cs *clientImpl) HandleStream(view database.View, btc *buffer.Batch) error {
	err := cs.WriteBatch(cs.context, view, btc)
	if err != nil {
		// If there is an acceptable error and if the functionality of resending data is activated,
		// try to repeat the operation
		if cs.options.isRetryEnabled && database.IsResendAvailable(err) {
			cs.retry.Retry(&retryPacket{
				view: view,
				btc:  btc,
			})
		}
		return err
	}
	return nil
}

func (cs *clientImpl) WriteBatch(ctx context.Context, view database.View, batch *buffer.Batch) error {
	_, err := cs.clickhouse.Insert(ctx, view, batch.Rows())
	return err
}

func (cs *clientImpl) RetryClient() Retryable {
	return cs.retry
}

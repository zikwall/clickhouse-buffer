package clickhousebuffer

import (
	"context"
	"sync"

	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

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
		client.retry = NewRetry(ctx, NewDefaultWriter(clickhouse), options.logger, options.isDebug)
	}
	return client
}

func (cs *clientImpl) Options() *Options {
	return cs.options
}

func (cs *clientImpl) Writer(view View, buf buffer.Buffer) Writer {
	key := view.Name
	cs.mu.Lock()
	if _, ok := cs.writeAPIs[key]; !ok {
		cs.writeAPIs[key] = NewWriter(cs, view, buf, cs.options)
	}
	writer := cs.writeAPIs[key]
	cs.mu.Unlock()
	return writer
}

func (cs *clientImpl) WriterBlocking(view View) WriterBlocking {
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
	}
	cs.mu.RLock()
	apisSnapshot := cs.writeAPIs
	cs.mu.RUnlock()
	if cs.options.isDebug {
		cs.logger.Log("close async writers")
	}
	for key, w := range apisSnapshot {
		w.Close()
		cs.mu.Lock()
		delete(cs.writeAPIs, key)
		cs.mu.Unlock()
	}
	if cs.options.isDebug {
		cs.logger.Log("close sync writers")
	}
	cs.mu.Lock()
	for key := range cs.syncWriteAPIs {
		delete(cs.syncWriteAPIs, key)
	}
	cs.mu.Unlock()
}

func (cs *clientImpl) HandleStream(view View, btc *buffer.Batch) error {
	err := cs.WriteBatch(cs.context, view, btc)
	if err != nil {
		if cs.options.isRetryEnabled && isResendAvailable(err) {
			cs.retry.Queue(&retryPacket{
				view: view,
				btc:  btc,
			})
		}
		return err
	}
	return nil
}

func (cs *clientImpl) WriteBatch(ctx context.Context, view View, batch *buffer.Batch) error {
	_, err := cs.clickhouse.Insert(ctx, view, batch.Rows())
	return err
}

func (cs *clientImpl) RetryClient() Retryable {
	return cs.retry
}

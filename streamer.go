package clickhouse_buffer

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/api"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"log"
	"sync"
)

type clientImpl struct {
	context       context.Context
	clickhouse    api.Clickhouse
	options       *api.Options
	writeAPIs     map[string]api.Writer
	syncWriteAPIs map[string]api.WriterBlocking
	mu            sync.RWMutex
}

func NewClient(ctx context.Context, clickhouse api.Clickhouse) api.Client {
	return NewClientWithOptions(ctx, clickhouse, api.DefaultOptions())
}

func NewClientWithOptions(ctx context.Context, clickhouse api.Clickhouse, options *api.Options) api.Client {
	client := &clientImpl{
		context:       ctx,
		clickhouse:    clickhouse,
		options:       options,
		writeAPIs:     map[string]api.Writer{},
		syncWriteAPIs: map[string]api.WriterBlocking{},
	}

	return client
}

func (cs *clientImpl) Options() *api.Options {
	return cs.options
}

func (cs *clientImpl) Writer(view api.View, buffer buffer.Buffer) api.Writer {
	key := view.Name
	cs.mu.Lock()
	if _, ok := cs.writeAPIs[key]; !ok {
		cs.writeAPIs[key] = api.NewWriter(cs, view, buffer, cs.options)
	}
	writer := cs.writeAPIs[key]
	cs.mu.Unlock()

	return writer
}

func (cs *clientImpl) WriterBlocking(view api.View) api.WriterBlocking {
	key := view.Name
	cs.mu.Lock()
	if _, ok := cs.syncWriteAPIs[key]; !ok {
		cs.syncWriteAPIs[key] = api.NewWriterBlocking(cs, view)
	}
	writer := cs.syncWriteAPIs[key]
	cs.mu.Unlock()

	return writer
}

func (cs *clientImpl) Close() {
	cs.mu.RLock()
	apisSnapshot := cs.writeAPIs
	cs.mu.RUnlock()

	for key, w := range apisSnapshot {
		if wa, ok := w.(*api.WriterImpl); ok {
			wa.Close()
		}

		cs.mu.Lock()
		delete(cs.writeAPIs, key)
		cs.mu.Unlock()
	}

	cs.mu.Lock()
	for key := range cs.syncWriteAPIs {
		delete(cs.syncWriteAPIs, key)
	}
	cs.mu.Unlock()
}

func (cs *clientImpl) HandleStream(view api.View, btc *api.Batch) error {
	err := cs.WriteBatch(cs.context, view, btc)
	if err != nil {
		// In the future, you need to add the possibility of repeating failed packets,
		// with limits and repetition intervals
		log.Println(err)
		return err
	}

	return nil
}

func (cs *clientImpl) WriteBatch(ctx context.Context, view api.View, btc *api.Batch) error {
	_, err := cs.clickhouse.Insert(ctx, view, btc.Rows())
	return err
}

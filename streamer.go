package clickhouse_buffer

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/api"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"log"
	"sync"
)

type clientImpl struct {
	context    context.Context
	cancel     context.CancelFunc
	clickhouse api.Clickhouse
	options    *api.Options
	writeAPIs  map[string]api.Writer
	mu         sync.RWMutex
}

func NewClient(context context.Context, clickhouse api.Clickhouse) api.Client {
	return NewClientWithOptions(context, clickhouse, api.DefaultOptions())
}

func NewClientWithOptions(ctx context.Context, clickhouse api.Clickhouse, options *api.Options) api.Client {
	client := &clientImpl{
		clickhouse: clickhouse,
		options:    options,
		writeAPIs:  map[string]api.Writer{},
	}
	client.context, client.cancel = context.WithCancel(ctx)

	return client
}

func (cs *clientImpl) Options() *api.Options {
	return cs.options
}

func (cs *clientImpl) Writer(view api.View, buffer buffer.Buffer) api.Writer {
	key := view.Name
	cs.mu.Lock()
	if _, ok := cs.writeAPIs[key]; !ok {
		cs.writeAPIs[key] = api.NewWriter(cs.context, view, buffer, cs.options)
	}
	writer := cs.writeAPIs[key]
	cs.mu.Unlock()

	return writer
}

func (cs *clientImpl) Close() {
	cs.cancel()

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
}

func (cs *clientImpl) HandleStream(btc *api.Batch) error {
	err := cs.writeBatch(cs.context, btc)
	if err != nil {
		// In the future, you need to add the possibility of repeating failed packets,
		// with limits and repetition intervals
		log.Fatalln(err)
		return err
	}

	return nil
}

func (cs *clientImpl) writeBatch(context context.Context, btc *api.Batch) error {
	_, err := cs.clickhouse.Insert(context, btc.View(), btc.Vectors())
	return err
}

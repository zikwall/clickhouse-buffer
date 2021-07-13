package clickhousebuffer

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"log"
	"sync"
)

type clientImpl struct {
	context       context.Context
	clickhouse    Clickhouse
	options       *Options
	writeAPIs     map[string]Writer
	syncWriteAPIs map[string]WriterBlocking
	mu            sync.RWMutex
}

func NewClient(ctx context.Context, clickhouse Clickhouse) Client {
	return NewClientWithOptions(ctx, clickhouse, DefaultOptions())
}

func NewClientWithOptions(ctx context.Context, clickhouse Clickhouse, options *Options) Client {
	client := &clientImpl{
		context:       ctx,
		clickhouse:    clickhouse,
		options:       options,
		writeAPIs:     map[string]Writer{},
		syncWriteAPIs: map[string]WriterBlocking{},
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
	cs.mu.RLock()
	apisSnapshot := cs.writeAPIs
	cs.mu.RUnlock()

	for key, w := range apisSnapshot {
		if wa, ok := w.(*WriterImpl); ok {
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

func (cs *clientImpl) HandleStream(view View, btc *Batch) error {
	err := cs.WriteBatch(cs.context, view, btc)
	if err != nil {
		// In the future, you need to add the possibility of repeating failed packets,
		// with limits and repetition intervals
		log.Println(err)
		return err
	}

	return nil
}

func (cs *clientImpl) WriteBatch(ctx context.Context, view View, btc *Batch) error {
	_, err := cs.clickhouse.Insert(ctx, view, btc.Rows())
	return err
}

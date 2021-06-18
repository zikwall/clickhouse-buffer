package clickhouse_buffer

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/api"
	"github.com/zikwall/clickhouse-buffer/src/batch"
)

type clientImpl struct {
	context    context.Context
	clickhouse api.Clickhouse
	options    *api.Options
}

func NewClient(context context.Context) api.Client {
	return NewClientWithOptions(context, api.DefaultOptions())
}

func NewClientWithOptions(context context.Context, options *api.Options) api.Client {
	client := &clientImpl{
		options: options,
		context: context,
	}

	return client
}

func (cs *clientImpl) Writer(view api.View) api.Writer {
	return api.NewWriter(view, cs.options)
}

func (cs *clientImpl) HandleStream(btc *batch.Batch) error {
	for {
		select {
		case <-cs.context.Done():
			return nil
		default:
		}

		err := cs.writeBatch(cs.context, btc)
		if err != nil {

		}
	}
}

func (cs *clientImpl) writeBatch(context context.Context, btc *batch.Batch) error {
	_, err := cs.clickhouse.Insert(context, btc.View(), btc.Vectors())
	return err
}

package internal

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/batch"
	"github.com/zikwall/clickhouse-buffer/src/clickhouse"
)

type ClickhouseStreamerImpl struct {
	clickhouse clickhouse.Clickhouse
}

func (cs *ClickhouseStreamerImpl) HandleStream(context context.Context, btc *batch.Batch) error {
	for {
		select {
		case <-context.Done():
			return nil
		default:
		}

		err := cs.writeBatch(context, btc)
		if err != nil {

		}
	}
}

func (cs *ClickhouseStreamerImpl) writeBatch(context context.Context, btc *batch.Batch) error {
	_, err := cs.clickhouse.Insert(context, btc.Table(), btc.Vectors())
	return err
}

package clickhousebuffer

import (
	"context"

	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type Retryable interface {
	Queue(packet *retryPacket)
	Metrics() (uint64, uint64, uint64)
}

type Writeable interface {
	Write(ctx context.Context, view View, batch *buffer.Batch) (uint64, error)
}

type retryPacket struct {
	view     View
	btc      *buffer.Batch
	tryCount uint8
}

type defaultWriter struct {
	conn Clickhouse
}

func NewDefaultWriter(conn Clickhouse) Writeable {
	w := &defaultWriter{
		conn: conn,
	}
	return w
}

func (w *defaultWriter) Write(ctx context.Context, view View, batch *buffer.Batch) (uint64, error) {
	affected, err := w.conn.Insert(ctx, view, batch.Rows())
	return affected, err
}

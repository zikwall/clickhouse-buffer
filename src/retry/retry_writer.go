package retry

import (
	"context"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

type Writeable interface {
	Write(ctx context.Context, view cx.View, batch *cx.Batch) (uint64, error)
}

type defaultWriter struct {
	conn cx.Clickhouse
}

func NewDefaultWriter(conn cx.Clickhouse) Writeable {
	w := &defaultWriter{
		conn: conn,
	}
	return w
}

func (w *defaultWriter) Write(ctx context.Context, view cx.View, batch *cx.Batch) (uint64, error) {
	affected, err := w.conn.Insert(ctx, view, batch.Rows())
	return affected, err
}

package clickhousebuffer

import (
	"context"

	"github.com/zikwall/clickhouse-buffer/database"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type Writeable interface {
	Write(ctx context.Context, view database.View, batch *buffer.Batch) (uint64, error)
}

type defaultWriter struct {
	conn database.Clickhouse
}

func NewDefaultWriter(conn database.Clickhouse) Writeable {
	w := &defaultWriter{
		conn: conn,
	}
	return w
}

func (w *defaultWriter) Write(ctx context.Context, view database.View, batch *buffer.Batch) (uint64, error) {
	affected, err := w.conn.Insert(ctx, view, batch.Rows())
	return affected, err
}

package clickhousebuffer

import (
	"context"

	"github.com/zikwall/clickhouse-buffer/v3/src/cx"
)

type WriterBlocking interface {
	// WriteRow writes row(s) into bucket.
	// WriteRow writes without implicit batching. Batch is created from given number of records
	// Non-blocking alternative is available in the Writer interface
	WriteRow(ctx context.Context, row ...cx.Vectorable) error
}

type writerBlocking struct {
	view   cx.View
	client Client
}

func NewWriterBlocking(client Client, view cx.View) WriterBlocking {
	w := &writerBlocking{
		view:   view,
		client: client,
	}
	return w
}

func (w *writerBlocking) WriteRow(ctx context.Context, row ...cx.Vectorable) error {
	if len(row) > 0 {
		rows := make([]cx.Vector, 0, len(row))
		for _, r := range row {
			rows = append(rows, r.Row())
		}
		return w.write(ctx, rows)
	}
	return nil
}

func (w *writerBlocking) write(ctx context.Context, rows []cx.Vector) error {
	err := w.client.WriteBatch(ctx, w.view, cx.NewBatch(rows))
	if err != nil {
		return err
	}
	return nil
}

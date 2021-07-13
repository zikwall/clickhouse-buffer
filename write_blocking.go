package clickhousebuffer

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type WriterBlocking interface {
	// WriteRow writes row(s) into bucket.
	// WriteRow writes without implicit batching. Batch is created from given number of records
	// Non-blocking alternative is available in the Writer interface
	WriteRow(ctx context.Context, row ...buffer.Inline) error
}

type WriterBlockingImpl struct {
	view     View
	streamer Client
}

func NewWriterBlocking(streamer Client, view View) WriterBlocking {
	return &WriterBlockingImpl{
		view:     view,
		streamer: streamer,
	}
}

func (w *WriterBlockingImpl) WriteRow(ctx context.Context, row ...buffer.Inline) error {
	if len(row) > 0 {
		rows := make([]buffer.RowSlice, 0, len(row))
		for _, r := range row {
			rows = append(rows, r.Row())
		}

		return w.write(ctx, rows)
	}

	return nil
}

func (w *WriterBlockingImpl) write(ctx context.Context, rows []buffer.RowSlice) error {
	err := w.streamer.WriteBatch(ctx, w.view, buffer.NewBatch(rows))

	if err != nil {
		return err
	}

	return nil
}

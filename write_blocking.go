package clickhousebuffer

import (
	"context"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

// WriterBlocking similar to Writer except that this interface must implement a blocking entry.
// WriterBlocking do not worry about errors and repeated entries of undelivered messages,
// all responsibility for error handling falls on developer
type WriterBlocking interface {
	// WriteRow writes row(s) into bucket.
	// WriteRow writes without implicit batching. Batch is created from given number of records
	// Non-blocking alternative is available in the Writer interface
	WriteRow(ctx context.Context, row ...cx.Vectorable) error
}

// writerBlocking structure implements the WriterBlocking interface and encapsulates all necessary logic within itself
type writerBlocking struct {
	view   cx.View
	client Client
}

// NewWriterBlocking WriterBlocking object
func NewWriterBlocking(client Client, view cx.View) WriterBlocking {
	w := &writerBlocking{
		view:   view,
		client: client,
	}
	return w
}

// WriteRow similar to WriteRow,
// only it is blocking and has the ability to write a large batch of data directly to the database at once
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

// write to Clickhouse database
func (w *writerBlocking) write(ctx context.Context, rows []cx.Vector) error {
	err := w.client.WriteBatch(ctx, w.view, cx.NewBatch(rows))
	if err != nil {
		return err
	}
	return nil
}

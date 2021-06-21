package api

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type Client interface {
	// Options returns the options associated with client
	Options() *Options
	HandleStream(*Batch) error
	WriteBatch(ctx context.Context, batch *Batch) error
	// Writer returns the asynchronous, non-blocking, Write client.
	// Ensures using a single Writer instance for each table pair.
	Writer(View, buffer.Buffer) Writer
	WriterBlocking(View) WriterBlocking
	// Close ensures all ongoing asynchronous write clients finish.
	Close()
}

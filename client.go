package clickhousebuffer

import (
	"context"

	"github.com/zikwall/clickhouse-buffer/database"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type Client interface {
	// Options returns the options associated with client
	Options() *Options
	// HandleStream method for processing data x and sending it to Clickhouse
	HandleStream(database.View, *buffer.Batch) error
	// WriteBatch method of sending data to Clickhouse is used implicitly in a non - blocking record,
	// and explicitly in a blocking record
	WriteBatch(context.Context, database.View, *buffer.Batch) error
	// Writer returns the asynchronous, non-blocking, Writer client.
	// Ensures using a single Writer instance for each table pair.
	Writer(database.View, buffer.Buffer) Writer
	// WriterBlocking returns the synchronous, blocking, WriterBlocking client.
	// Ensures using a single WriterBlocking instance for each table pair.
	WriterBlocking(database.View) WriterBlocking
	// RetryClient Get retry client
	RetryClient() Retryable
	// Close ensures all ongoing asynchronous write clients finish.
	Close()
}

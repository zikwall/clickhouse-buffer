package api

import "github.com/zikwall/clickhouse-buffer/src/buffer"

type Client interface {
	HandleStream(*Batch) error
	// Writer returns the asynchronous, non-blocking, Write client.
	// Ensures using a single Writer instance for each table pair.
	Writer(View, buffer.Buffer) Writer
	// Close ensures all ongoing asynchronous write clients finish.
	Close()
}

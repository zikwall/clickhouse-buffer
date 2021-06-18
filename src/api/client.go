package api

import (
	"github.com/zikwall/clickhouse-buffer/src/batch"
)

type Client interface {
	HandleStream(*batch.Batch) error
	// Writer returns the asynchronous, non-blocking, Write client.
	// Ensures using a single Writer instance for each table pair.
	Writer(view View) Writer
}

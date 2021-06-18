package internal

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/batch"
)

type BufferStreamingWriter interface {
	HandleStream(context.Context, *batch.Batch) error
}

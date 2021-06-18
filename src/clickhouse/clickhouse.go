package clickhouse

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/batch"
)

type Clickhouse interface {
	Insert(context.Context, View, []batch.Vector) (uint64, error)
}

type View struct {
	Name    string
	Columns []string
}

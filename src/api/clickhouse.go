package api

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/common"
)

type Clickhouse interface {
	Insert(context.Context, View, []common.Vector) (uint64, error)
}

type View struct {
	Name    string
	Columns []string
}

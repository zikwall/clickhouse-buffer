package api

import (
	"context"
)

type Clickhouse interface {
	Insert(context.Context, View, []Vector) (uint64, error)
}

type View struct {
	Name    string
	Columns []string
}

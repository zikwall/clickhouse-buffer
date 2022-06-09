package cx

import (
	"context"
)

type View struct {
	Name    string
	Columns []string
}

func NewView(name string, columns []string) View {
	return View{Name: name, Columns: columns}
}

type Clickhouse interface {
	Insert(context.Context, View, []Vector) (uint64, error)
	Close() error
}

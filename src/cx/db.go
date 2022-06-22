package cx

import (
	"context"
)

// View basic representation is some reflection of the entity (table) in Clickhouse database
type View struct {
	Name    string
	Columns []string
}

// NewView return View
func NewView(name string, columns []string) View {
	return View{Name: name, Columns: columns}
}

// Clickhouse base interface, which is inherited by the top-level Client API and further by all its child Writer-s
type Clickhouse interface {
	Insert(context.Context, View, []Vector) (uint64, error)
	Close() error
}

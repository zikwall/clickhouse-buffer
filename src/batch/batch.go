package batch

import (
	"github.com/zikwall/clickhouse-buffer/src/api"
)

type Scalar interface {
	Vector() []interface{}
}

type Vector []interface{}

// Batch holds information for sending rows batch
type Batch struct {
	view    api.View
	vectors []Vector
}

// NewBatch creates new batch
func NewBatch(vectors []Vector) *Batch {
	return &Batch{
		vectors: vectors,
	}
}

func (b *Batch) View() api.View {
	return b.view
}

func (b *Batch) Vectors() []Vector {
	return b.vectors
}

package api

import "github.com/zikwall/clickhouse-buffer/src/common"

// Batch holds information for sending rows batch
type Batch struct {
	view    View
	vectors []common.Vector
}

// NewBatch creates new batch
func NewBatch(vectors []common.Vector) *Batch {
	return &Batch{
		vectors: vectors,
	}
}

func (b *Batch) View() View {
	return b.view
}

func (b *Batch) Vectors() []common.Vector {
	return b.vectors
}

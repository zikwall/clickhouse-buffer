package api

import "github.com/zikwall/clickhouse-buffer/src/types"

// Batch holds information for sending rows batch
type Batch struct {
	view View
	rows []types.RowSlice
}

// NewBatch creates new batch
func NewBatch(rows []types.RowSlice) *Batch {
	return &Batch{
		rows: rows,
	}
}

func (b *Batch) View() View {
	return b.view
}

func (b *Batch) Rows() []types.RowSlice {
	return b.rows
}

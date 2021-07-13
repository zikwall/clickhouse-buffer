package clickhousebuffer

import "github.com/zikwall/clickhouse-buffer/src/buffer"

// Batch holds information for sending rows batch
type Batch struct {
	rows []buffer.RowSlice
}

// NewBatch creates new batch
func NewBatch(rows []buffer.RowSlice) *Batch {
	return &Batch{
		rows: rows,
	}
}

func (b *Batch) Rows() []buffer.RowSlice {
	return b.rows
}

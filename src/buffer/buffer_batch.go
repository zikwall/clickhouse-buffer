package buffer

// Batch holds information for sending rows batch
type Batch struct {
	rows []RowSlice
}

// NewBatch creates new batch
func NewBatch(rows []RowSlice) *Batch {
	return &Batch{
		rows: rows,
	}
}

func (b *Batch) Rows() []RowSlice {
	return b.rows
}

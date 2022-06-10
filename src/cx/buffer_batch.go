package cx

// Batch holds information for sending rows batch
type Batch struct {
	rows []Vector
}

// NewBatch creates new batch
func NewBatch(rows []Vector) *Batch {
	return &Batch{
		rows: rows,
	}
}

func (b *Batch) Rows() []Vector {
	return b.rows
}

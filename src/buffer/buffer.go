package buffer

// Buffer it is the interface for creating a data buffer (temporary storage).
// It is enough to implement this interface so that you can use your own temporary storage
type Buffer interface {
	Write(RowSlice)
	Read() []RowSlice
	Len() int
	Flush()
}

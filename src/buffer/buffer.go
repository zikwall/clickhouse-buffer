package buffer

import (
	"github.com/zikwall/clickhouse-buffer/src/types"
)

// Buffer it is the interface for creating a data buffer (temporary storage).
// It is enough to implement this interface so that you can use your own temporary storage
type Buffer interface {
	Write(types.RowSlice)
	Read() []types.RowSlice
	Len() int
	Flush()
}

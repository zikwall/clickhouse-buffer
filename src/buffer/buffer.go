package buffer

import (
	"github.com/zikwall/clickhouse-buffer/src/types"
)

type Buffer interface {
	Write(types.RowSlice)
	Read() []types.RowSlice
	Len() int
	Flush()
}

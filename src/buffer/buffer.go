package buffer

import (
	"github.com/zikwall/clickhouse-buffer/src/common"
)

type Buffer interface {
	Write(vector common.Vector)
	Read() []common.Vector
	Len() int
	Flush()
}

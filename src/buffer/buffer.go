package buffer

import (
	"github.com/zikwall/clickhouse-buffer/src/common"
)

type Buffer interface {
	Write(vector common.Vector)
	Buffer() []common.Vector
	Len() int
	Flush()
}

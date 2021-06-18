package buffer

import "github.com/zikwall/clickhouse-buffer/src/api"

type Buffer interface {
	Write(vector api.Vector)
	Buffer() []api.Vector
	Len() int
	Flush()
}

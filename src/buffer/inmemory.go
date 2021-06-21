package buffer

import (
	"github.com/zikwall/clickhouse-buffer/src/types"
)

type InMemoryBuffer struct {
	writeBuffer []types.RowSlice
}

func NewInmemoryBuffer(bufferSize uint) *InMemoryBuffer {
	return &InMemoryBuffer{
		writeBuffer: make([]types.RowSlice, 0, bufferSize+1),
	}
}

func (in *InMemoryBuffer) Write(row types.RowSlice) {
	in.writeBuffer = append(in.writeBuffer, row)
}

func (in *InMemoryBuffer) Read() []types.RowSlice {
	return in.writeBuffer
}

func (in *InMemoryBuffer) Len() int {
	return len(in.writeBuffer)
}

func (in *InMemoryBuffer) Flush() {
	in.writeBuffer = in.writeBuffer[:0]
}

package buffer

import (
	"github.com/zikwall/clickhouse-buffer/src/common"
)

type InMemoryBuffer struct {
	writeBuffer []common.Vector
}

func NewInmemoryBuffer(bufferSize uint) *InMemoryBuffer {
	return &InMemoryBuffer{
		writeBuffer: make([]common.Vector, 0, bufferSize+1),
	}
}

func (in *InMemoryBuffer) Write(vector common.Vector) {
	in.writeBuffer = append(in.writeBuffer, vector)
}

func (in *InMemoryBuffer) Read() []common.Vector {
	return in.writeBuffer
}

func (in *InMemoryBuffer) Len() int {
	return len(in.writeBuffer)
}

func (in *InMemoryBuffer) Flush() {
	in.writeBuffer = in.writeBuffer[:0]
}

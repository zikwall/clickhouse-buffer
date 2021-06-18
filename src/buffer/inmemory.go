package buffer

import "github.com/zikwall/clickhouse-buffer/src/api"

type InMemoryBuffer struct {
	writeBuffer []api.Vector
}

func NewInmemoryBuffer(bufferSize int) *InMemoryBuffer {
	return &InMemoryBuffer{
		writeBuffer: make([]api.Vector, 0, bufferSize+1),
	}
}

func (in *InMemoryBuffer) Write(vector api.Vector) {
	in.writeBuffer = append(in.writeBuffer, vector)
}

func (in *InMemoryBuffer) Buffer() []api.Vector {
	return in.writeBuffer
}

func (in *InMemoryBuffer) Len() int {
	return len(in.writeBuffer)
}

func (in *InMemoryBuffer) Flush() {
	in.writeBuffer = in.writeBuffer[:0]
}

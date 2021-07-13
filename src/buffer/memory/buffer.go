package memory

import (
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type inMemoryBufferImpl struct {
	writeBuffer []buffer.RowSlice
}

func NewBuffer(bufferSize uint) buffer.Buffer {
	return &inMemoryBufferImpl{
		writeBuffer: make([]buffer.RowSlice, 0, bufferSize+1),
	}
}

func (in *inMemoryBufferImpl) Write(row buffer.RowSlice) {
	in.writeBuffer = append(in.writeBuffer, row)
}

func (in *inMemoryBufferImpl) Read() []buffer.RowSlice {
	return in.writeBuffer
}

func (in *inMemoryBufferImpl) Len() int {
	return len(in.writeBuffer)
}

func (in *inMemoryBufferImpl) Flush() {
	in.writeBuffer = in.writeBuffer[:0]
}

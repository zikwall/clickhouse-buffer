package memory

import (
	"github.com/zikwall/clickhouse-buffer/src/types"
)

type Buffer struct {
	writeBuffer []types.RowSlice
}

func NewBuffer(bufferSize uint) *Buffer {
	return &Buffer{
		writeBuffer: make([]types.RowSlice, 0, bufferSize+1),
	}
}

func (in *Buffer) Write(row types.RowSlice) {
	in.writeBuffer = append(in.writeBuffer, row)
}

func (in *Buffer) Read() []types.RowSlice {
	return in.writeBuffer
}

func (in *Buffer) Len() int {
	return len(in.writeBuffer)
}

func (in *Buffer) Flush() {
	in.writeBuffer = in.writeBuffer[:0]
}

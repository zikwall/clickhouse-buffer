package memory

import (
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type Buffer struct {
	writeBuffer []buffer.RowSlice
}

func NewBuffer(bufferSize uint) *Buffer {
	return &Buffer{
		writeBuffer: make([]buffer.RowSlice, 0, bufferSize+1),
	}
}

func (in *Buffer) Write(row buffer.RowSlice) {
	in.writeBuffer = append(in.writeBuffer, row)
}

func (in *Buffer) Read() []buffer.RowSlice {
	return in.writeBuffer
}

func (in *Buffer) Len() int {
	return len(in.writeBuffer)
}

func (in *Buffer) Flush() {
	in.writeBuffer = in.writeBuffer[:0]
}

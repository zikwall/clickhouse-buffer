package memory

import (
	"github.com/zikwall/clickhouse-buffer/v2/src/buffer"
)

type memory struct {
	buffer []buffer.RowSlice
	size   uint
}

func NewBuffer(bufferSize uint) buffer.Buffer {
	return &memory{
		buffer: make([]buffer.RowSlice, 0, bufferSize+1),
		size:   bufferSize + 1,
	}
}

func (i *memory) Write(row buffer.RowSlice) {
	i.buffer = append(i.buffer, row)
}

func (i *memory) Read() []buffer.RowSlice {
	return i.buffer
}

func (i *memory) Len() int {
	return len(i.buffer)
}

func (i *memory) Flush() {
	i.buffer = make([]buffer.RowSlice, 0, i.size)
}

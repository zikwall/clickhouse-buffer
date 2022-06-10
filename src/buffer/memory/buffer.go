package memory

import (
	"github.com/zikwall/clickhouse-buffer/src/buffer"
)

type memory struct {
	buffer []buffer.RowSlice
	size   uint
}

func NewBuffer(bufferSize uint) buffer.Buffer {
	size := bufferSize + 1
	return &memory{
		buffer: make([]buffer.RowSlice, 0, size),
		size:   size,
	}
}

func (i *memory) Write(row buffer.RowSlice) {
	i.buffer = append(i.buffer, row)
}

func (i *memory) Read() []buffer.RowSlice {
	snapshot := make([]buffer.RowSlice, len(i.buffer))
	copy(snapshot, i.buffer)
	return snapshot
}

func (i *memory) Len() int {
	return len(i.buffer)
}

func (i *memory) Flush() {
	i.buffer = i.buffer[:0]
}

package cxmem

import (
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

type memory struct {
	buffer []cx.Vector
	size   uint
}

func NewBuffer(bufferSize uint) cx.Buffer {
	return &memory{
		buffer: make([]cx.Vector, 0, bufferSize+1),
		size:   bufferSize + 1,
	}
}

func (i *memory) Write(row cx.Vector) {
	i.buffer = append(i.buffer, row)
}

func (i *memory) Read() []cx.Vector {
	snapshot := make([]cx.Vector, len(i.buffer))
	copy(snapshot, i.buffer)
	return snapshot
}

func (i *memory) Len() int {
	return len(i.buffer)
}

func (i *memory) Flush() {
	i.buffer = i.buffer[:0]
}

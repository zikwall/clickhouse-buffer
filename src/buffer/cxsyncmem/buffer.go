package cxsyncmem

import (
	"sync"

	"github.com/zikwall/clickhouse-buffer/v2/src/cx"
)

// special for tests with locks
type memory struct {
	buffer []cx.Vector
	size   uint
	mu     *sync.RWMutex
}

func NewBuffer(bufferSize uint) cx.Buffer {
	return &memory{
		buffer: make([]cx.Vector, 0, bufferSize+1),
		size:   bufferSize + 1,
		mu:     &sync.RWMutex{},
	}
}

func (i *memory) Write(row cx.Vector) {
	i.mu.Lock()
	i.buffer = append(i.buffer, row)
	i.mu.Unlock()
}

func (i *memory) Read() []cx.Vector {
	i.mu.RLock()
	snapshot := make([]cx.Vector, len(i.buffer))
	copy(snapshot, i.buffer)
	i.mu.RUnlock()
	return snapshot
}

func (i *memory) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.buffer)
}

func (i *memory) Flush() {
	i.mu.Lock()
	i.buffer = i.buffer[:0]
	i.mu.Unlock()
}

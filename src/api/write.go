package api

import (
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"github.com/zikwall/clickhouse-buffer/src/types"
	"time"
)

// Writer is client interface with non-blocking methods for writing rows asynchronously in batches into an Clickhouse server.
// Writer can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type Writer interface {
	// WriteVector writes asynchronously line protocol record into bucket.
	WriteRow(vector types.Rower)
	// Flush forces all pending writes from the buffer to be sent
	Flush()
	// Errors returns a channel for reading errors which occurs during async writes.
	Errors() <-chan error
}

type WriterImpl struct {
	view         View
	streamer     Client
	writeBuffer  buffer.Buffer
	writeCh      chan *Batch
	errCh        chan error
	bufferCh     chan types.RowSlice
	bufferFlush  chan struct{}
	doneCh       chan struct{}
	writeStop    chan struct{}
	bufferStop   chan struct{}
	writeOptions *Options
}

// NewWriter returns new non-blocking write client for writing rows to Clickhouse table
func NewWriter(client Client, view View, buffer buffer.Buffer, writeOptions *Options) *WriterImpl {
	w := &WriterImpl{
		view:         view,
		streamer:     client,
		writeBuffer:  buffer,
		writeOptions: writeOptions,
		writeCh:      make(chan *Batch),
		bufferCh:     make(chan types.RowSlice),
		bufferFlush:  make(chan struct{}),
		doneCh:       make(chan struct{}),
		bufferStop:   make(chan struct{}),
		writeStop:    make(chan struct{}),
	}

	go w.listenBufferWrite()
	go w.listenStreamWrite()

	return w
}

// WriteRow writes asynchronously line protocol record into bucket.
// WriteRow adds record into the buffer which is sent on the background when it reaches the batch size.
func (w *WriterImpl) WriteRow(scalar types.Rower) {
	w.bufferCh <- scalar.Row()
}

func (w *WriterImpl) flushBuffer() {
	if w.writeBuffer.Len() > 0 {
		w.writeCh <- NewBatch(w.writeBuffer.Read())
		w.writeBuffer.Flush()
	}
}

func (w *WriterImpl) listenBufferWrite() {
	ticker := time.NewTicker(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)

	for {
		select {
		case vector := <-w.bufferCh:
			w.writeBuffer.Write(vector)
			if w.writeBuffer.Len() == int(w.writeOptions.BatchSize()) {
				w.flushBuffer()
			}
		case <-w.bufferStop:
			w.flushBuffer()
			w.doneCh <- struct{}{}
			ticker.Stop()
			return
		case <-ticker.C:
			w.flushBuffer()
		case <-w.bufferFlush:
			w.flushBuffer()
		}
	}
}

func (w *WriterImpl) listenStreamWrite() {
	for {
		select {
		case btc := <-w.writeCh:
			err := w.streamer.HandleStream(btc)
			if err != nil && w.errCh != nil {
				w.errCh <- err
			}
		case <-w.writeStop:
			w.doneCh <- struct{}{}
			return
		}
	}
}

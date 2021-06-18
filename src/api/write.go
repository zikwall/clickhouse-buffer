package api

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/batch"
	"github.com/zikwall/clickhouse-buffer/src/clickhouse"
	"github.com/zikwall/clickhouse-buffer/src/internal"
	"time"
)

// Writer is client interface with non-blocking methods for writing rows asynchronously in batches into an Clickhouse server.
// Writer can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type Writer interface {
	// WriteVector writes asynchronously line protocol record into bucket.
	WriteVector(vector batch.Vector)
	// Flush forces all pending writes from the buffer to be sent
	Flush()
	// Errors returns a channel for reading errors which occurs during async writes.
	Errors() <-chan error
}

type WriterImpl struct {
	context      context.Context
	view         clickhouse.View
	streamer     internal.BufferStreamingWriter
	writeBuffer  []batch.Vector
	writeCh      chan *batch.Batch
	errCh        chan error
	bufferCh     chan batch.Vector
	writeStop    chan struct{}
	bufferStop   chan struct{}
	bufferFlush  chan struct{}
	doneCh       chan struct{}
	writeOptions *Options
}

// NewWrite returns new non-blocking write client for writing rows to Clickhouse table
func NewWrite(view clickhouse.View, writeOptions *Options) *WriterImpl {
	w := &WriterImpl{
		writeBuffer:  make([]batch.Vector, 0, writeOptions.BatchSize()+1),
		writeCh:      make(chan *batch.Batch),
		bufferCh:     make(chan batch.Vector),
		bufferStop:   make(chan struct{}),
		writeStop:    make(chan struct{}),
		bufferFlush:  make(chan struct{}),
		doneCh:       make(chan struct{}),
		writeOptions: writeOptions,
	}

	go w.listenBufferWrite()
	go w.listenStreamWrite()

	return w
}

// WriteVector writes asynchronously line protocol record into bucket.
// WriteVector adds record into the buffer which is sent on the background when it reaches the batch size.
func (w *WriterImpl) WriteVector(scalar batch.Scalar) {
	w.bufferCh <- scalar.Vector()
}

func (w *WriterImpl) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		w.writeCh <- batch.NewBatch(w.writeBuffer)
		w.writeBuffer = w.writeBuffer[:0]
	}
}

func (w *WriterImpl) listenBufferWrite() {
	ticker := time.NewTicker(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)

	defer func() {
		w.doneCh <- struct{}{}
		ticker.Stop()
	}()

	for {
		select {
		case vector := <-w.bufferCh:
			w.writeBuffer = append(w.writeBuffer, vector)
			if len(w.writeBuffer) == int(w.writeOptions.BatchSize()) {
				w.flushBuffer()
			}
		case <-ticker.C:
			w.flushBuffer()
		case <-w.bufferFlush:
			w.flushBuffer()
		case <-w.bufferStop:
			ticker.Stop()
			w.flushBuffer()
			return
		}
	}
}

func (w *WriterImpl) listenStreamWrite() {
	defer func() {
		w.doneCh <- struct{}{}
	}()

	for {
		select {
		case btc := <-w.writeCh:
			err := w.streamer.HandleStream(w.context, btc)
			if err != nil && w.errCh != nil {
				w.errCh <- err
			}
		case <-w.writeStop:
			return
		}
	}
}

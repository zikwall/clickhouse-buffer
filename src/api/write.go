package api

import (
	"context"
	"time"
)

// Writer is client interface with non-blocking methods for writing rows asynchronously in batches into an Clickhouse server.
// Writer can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type Writer interface {
	// WriteVector writes asynchronously line protocol record into bucket.
	WriteVector(vector Scalar)
	// Flush forces all pending writes from the buffer to be sent
	Flush()
	// Errors returns a channel for reading errors which occurs during async writes.
	Errors() <-chan error
}

type WriterImpl struct {
	context      context.Context
	view         View
	streamer     Client
	writeBuffer  []Vector
	writeCh      chan *Batch
	errCh        chan error
	bufferCh     chan Vector
	bufferFlush  chan struct{}
	doneCh       chan struct{}
	writeOptions *Options
}

// NewWriter returns new non-blocking write client for writing rows to Clickhouse table
func NewWriter(context context.Context, view View, writeOptions *Options) *WriterImpl {
	w := &WriterImpl{
		writeBuffer:  make([]Vector, 0, writeOptions.BatchSize()+1),
		writeCh:      make(chan *Batch),
		bufferCh:     make(chan Vector),
		bufferFlush:  make(chan struct{}),
		doneCh:       make(chan struct{}),
		writeOptions: writeOptions,
		view:         view,
		context:      context,
	}

	go w.listenBufferWrite()
	go w.listenStreamWrite()

	return w
}

// WriteVector writes asynchronously line protocol record into bucket.
// WriteVector adds record into the buffer which is sent on the background when it reaches the batch size.
func (w *WriterImpl) WriteVector(scalar Scalar) {
	w.bufferCh <- scalar.Vector()
}

func (w *WriterImpl) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		w.writeCh <- NewBatch(w.writeBuffer)
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
		case <-w.context.Done():
			w.flushBuffer()
			return
		case <-ticker.C:
			w.flushBuffer()
		case <-w.bufferFlush:
			w.flushBuffer()
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
			err := w.streamer.HandleStream(btc)
			if err != nil && w.errCh != nil {
				w.errCh <- err
			}
		case <-w.context.Done():
			return
		}
	}
}

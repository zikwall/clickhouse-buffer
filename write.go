package clickhousebuffer

import (
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"time"
)

// Writer is client interface with non-blocking methods for writing rows asynchronously in batches into an Clickhouse server.
// Writer can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type Writer interface {
	// WriteRow writes asynchronously line protocol record into bucket.
	WriteRow(vector buffer.Inline)
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
	bufferCh     chan buffer.RowSlice
	bufferFlush  chan struct{}
	doneCh       chan struct{}
	writeStop    chan struct{}
	bufferStop   chan struct{}
	writeOptions *Options
}

// NewWriter returns new non-blocking write client for writing rows to Clickhouse table
func NewWriter(client Client, view View, buf buffer.Buffer, writeOptions *Options) Writer {
	w := &WriterImpl{
		view:         view,
		streamer:     client,
		writeBuffer:  buf,
		writeOptions: writeOptions,
		writeCh:      make(chan *Batch),
		bufferCh:     make(chan buffer.RowSlice),
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
func (w *WriterImpl) WriteRow(rower buffer.Inline) {
	w.bufferCh <- rower.Row()
}

// Errors returns a channel for reading errors which occurs during async writes.
// Must be called before performing any writes for errors to be collected.
// The chan is unbuffered and must be drained or the writer will block.
func (w *WriterImpl) Errors() <-chan error {
	if w.errCh == nil {
		w.errCh = make(chan error)
	}
	return w.errCh
}

// Flush forces all pending writes from the buffer to be sent
func (w *WriterImpl) Flush() {
	w.bufferFlush <- struct{}{}
	w.awaitFlushing()
}

func (w *WriterImpl) awaitFlushing() {
	// waiting buffer is flushed
	<-time.After(time.Millisecond)
}

// Close finishes outstanding write operations,
// stop background routines and closes all channels
func (w *WriterImpl) Close() {
	if w.writeCh != nil {
		// Flush outstanding metrics
		w.Flush()

		// stop and wait for write buffer
		close(w.bufferStop)
		<-w.doneCh

		close(w.bufferFlush)
		close(w.bufferCh)

		// stop and wait for write clickhouse
		close(w.writeStop)
		<-w.doneCh

		close(w.writeCh)
		w.writeCh = nil

		// close errors if open
		if w.errCh != nil {
			close(w.errCh)
			w.errCh = nil
		}
	}
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
			err := w.streamer.HandleStream(w.view, btc)
			if err != nil && w.errCh != nil {
				w.errCh <- err
			}
		case <-w.writeStop:
			w.doneCh <- struct{}{}
			return
		}
	}
}

package clickhousebuffer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

// Writer is client interface with non-blocking methods for writing rows asynchronously in batches into an Clickhouse server.
// Writer can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type Writer interface {
	// WriteRow writes asynchronously line protocol record into bucket.
	WriteRow(vector cx.Vectorable)
	// TryWriteRow same as WriteRow, but with Channel Closing Principle (Gracefully Close Channels)
	TryWriteRow(vec cx.Vectorable)
	// WriteVector writes asynchronously line protocol record into bucket.
	WriteVector(vec cx.Vector)
	// TryWriteVector same as WriteVector
	TryWriteVector(vec cx.Vector)
	// Errors returns a channel for reading errors which occurs during async writes.
	Errors() <-chan error
	// Close writer
	Close()
}

// writer structure implements the Writer interface,
// encapsulates all the necessary methods within itself and manages its own personal data flows
type writer struct {
	context      context.Context
	view         cx.View
	client       Client
	bufferEngine cx.Buffer
	writeOptions *Options
	errCh        chan error
	clickhouseCh chan *cx.Batch
	bufferCh     chan cx.Vector
	doneCh       chan struct{}
	writeStop    chan struct{}
	bufferStop   chan struct{}
	mu           *sync.RWMutex
	isOpenErr    int32
}

// NewWriter returns new non-blocking write client for writing rows to Clickhouse table
func NewWriter(ctx context.Context, client Client, view cx.View, engine cx.Buffer) Writer {
	w := &writer{
		mu:           &sync.RWMutex{},
		context:      ctx,
		view:         view,
		client:       client,
		bufferEngine: engine,
		writeOptions: client.Options(),
		// write buffers
		clickhouseCh: make(chan *cx.Batch),
		bufferCh:     make(chan cx.Vector),
		// signals
		doneCh:     make(chan struct{}),
		bufferStop: make(chan struct{}),
		writeStop:  make(chan struct{}),
	}
	go w.runBufferBridge()
	go w.runClickhouseBridge()
	return w
}

// WriteRow writes asynchronously line protocol record into bucket.
// WriteRow adds record into the buffer which is sent on the background when it reaches the batch size.
func (w *writer) WriteRow(vec cx.Vectorable) {
	// maybe use atomic for check is closed
	// atomic.LoadInt32(&w.isClosed) == 1
	w.bufferCh <- vec.Row()
}

// TryWriteRow same as WriteRow, but with Channel Closing Principle (Gracefully Close Channels)
func (w *writer) TryWriteRow(vec cx.Vectorable) {
	// the try-receive operation is to try to exit the goroutine as early as
	// possible.
	select {
	case <-w.bufferStop:
		return
	default:
	}
	// even if bufferStop is closed, the first branch in the second select may be
	// still not selected for some loops if to send to bufferCh is also unblocked.
	select {
	case <-w.bufferStop:
		return
	case w.bufferCh <- vec.Row():
	}
}

// WriteVector same as WriteRow, but just uses inlined vector.
// WriteVector a faster option for writing to buffer than WriteRow, in addition, memory allocates less
func (w *writer) WriteVector(vec cx.Vector) {
	w.bufferCh <- vec
}

// TryWriteVector same as WriteVector
func (w *writer) TryWriteVector(vec cx.Vector) {
	select {
	case <-w.bufferStop:
		return
	default:
	}
	select {
	case <-w.bufferStop:
		return
	case w.bufferCh <- vec:
	}
}

// Errors returns a channel for reading errors which occurs during async writes.
// Errors must be called before performing any writes for errors to be collected.
// Errors chan is unbuffered and must be drained or the writer will block.
func (w *writer) Errors() <-chan error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.errCh == nil {
		// mark that have a channel reader with errors so that can write to same channel
		atomic.StoreInt32(&w.isOpenErr, 1)
		w.errCh = make(chan error)
	}
	return w.errCh
}

// hasErrReader returns true if there is at least one channel reader with errors, otherwise false
func (w *writer) hasErrReader() bool {
	return atomic.LoadInt32(&w.isOpenErr) > 0
}

// Close finishes outstanding write operations, stop background routines and closes all channels
func (w *writer) Close() {
	if w.clickhouseCh != nil {
		// stop and wait for write buffer
		close(w.bufferStop)
		<-w.doneCh

		// stop and wait for write clickhouse
		close(w.writeStop)
		<-w.doneCh

		// stop ticker for flush to batch
		// close(w.tickerStop)
		// <-w.doneCh
	}
	if w.writeOptions.isDebug {
		w.writeOptions.logger.Logf("close writer %s", w.view.Name)
	}
}

// flush generates a new message packet and sends it to the queue channel for subsequent recording to Clickhouse database
func (w *writer) flush() {
	if w.writeOptions.isDebug {
		w.writeOptions.logger.Logf("flush buffer: %s", w.view.Name)
	}
	w.clickhouseCh <- cx.NewBatch(w.bufferEngine.Read())
	w.bufferEngine.Flush()
}

// func (w *writer) runTicker() {
//	ticker := time.NewTicker(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)
//	w.writeOptions.logger.Logf("run ticker: %s", w.view.Name)
//	defer func() {
//		ticker.Stop()
//		w.doneCh <- struct{}{}
//		w.writeOptions.logger.Logf("stop ticker: %s", w.view.Name)
//	}()
//	for {
//		select {
//		case <-ticker.C:
//			if w.bufferEngine.Len() > 0 {
//				w.flush()
//			}
//		case <-w.tickerStop:
//			return
//		}
//	}
// }

// runBufferBridge writing to a temporary buffer to collect more data
func (w *writer) runBufferBridge() {
	ticker := time.NewTicker(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)
	defer func() {
		ticker.Stop()
		// flush last data
		if w.bufferEngine.Len() > 0 {
			w.flush()
		}
		w.mu.Lock()
		// close buffer channel
		close(w.bufferCh)
		w.bufferCh = nil
		w.mu.Unlock()
		// send signal, buffer listener is done
		w.doneCh <- struct{}{}
		if w.writeOptions.isDebug {
			w.writeOptions.logger.Logf("stop buffer bridge: %s", w.view.Name)
		}
	}()
	if w.writeOptions.isDebug {
		w.writeOptions.logger.Logf("run buffer bridge: %s", w.view.Name)
	}
	for {
		select {
		case vector := <-w.bufferCh:
			w.bufferEngine.Write(vector)
			if w.bufferEngine.Len() == int(w.writeOptions.BatchSize()) {
				w.flush()
			}
		case <-w.bufferStop:
			return
		case <-ticker.C:
			if w.bufferEngine.Len() > 0 {
				w.flush()
			}
		}
	}
}

// runClickhouseBridge asynchronously write to Clickhouse database in large batches
func (w *writer) runClickhouseBridge() {
	if w.writeOptions.isDebug {
		w.writeOptions.logger.Logf("run clickhouse bridge: %s", w.view.Name)
	}
	defer func() {
		w.mu.Lock()
		// close clickhouse channel
		close(w.clickhouseCh)
		w.clickhouseCh = nil
		// close errors channel if it created
		if w.errCh != nil {
			close(w.errCh)
			w.errCh = nil
		}
		w.mu.Unlock()
		// send signal, clickhouse listener is done
		w.doneCh <- struct{}{}
		if w.writeOptions.isDebug {
			w.writeOptions.logger.Logf("stop clickhouse bridge: %s", w.view.Name)
		}
	}()
	for {
		select {
		case batch := <-w.clickhouseCh:
			err := w.client.WriteBatch(w.context, w.view, batch)
			if err != nil && w.hasErrReader() {
				w.errCh <- err
			}
		case <-w.writeStop:
			return
		}
	}
}

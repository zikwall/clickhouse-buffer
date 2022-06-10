package cx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Writer is client interface with non-blocking methods for writing rows asynchronously in batches into an Clickhouse server.
// Writer can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type Writer interface {
	// WriteRow writes asynchronously line protocol record into bucket.
	WriteRow(vector Vectorable)
	// Errors returns a channel for reading errors which occurs during async writes.
	Errors() <-chan error
	// Close writer
	Close()
}

type writer struct {
	context      context.Context
	view         View
	client       Client
	bufferEngine Buffer
	writeOptions *Options
	errCh        chan error
	clickhouseCh chan *Batch
	bufferCh     chan Vector
	doneCh       chan struct{}
	writeStop    chan struct{}
	bufferStop   chan struct{}
	mu           *sync.RWMutex
	isOpenErr    int32
}

// NewWriter returns new non-blocking write client for writing rows to Clickhouse table
func NewWriter(ctx context.Context, client Client, view View, engine Buffer) Writer {
	w := &writer{
		mu:           &sync.RWMutex{},
		context:      ctx,
		view:         view,
		client:       client,
		bufferEngine: engine,
		writeOptions: client.Options(),
		// write buffers
		clickhouseCh: make(chan *Batch),
		bufferCh:     make(chan Vector, 100),
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
func (w *writer) WriteRow(rower Vectorable) {
	// maybe use atomic for check is closed
	// atomic.LoadInt32(&w.isClosed) == 1
	w.bufferCh <- rower.Row()
}

// Errors returns a channel for reading errors which occurs during async writes.
// Must be called before performing any writes for errors to be collected.
// The chan is unbuffered and must be drained or the writer will block.
func (w *writer) Errors() <-chan error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.errCh == nil {
		atomic.StoreInt32(&w.isOpenErr, 1)
		w.errCh = make(chan error)
	}
	return w.errCh
}

func (w *writer) hasErrReader() bool {
	return atomic.LoadInt32(&w.isOpenErr) > 0
}

// Close finishes outstanding write operations,
// stop background routines and closes all channels
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

func (w *writer) flush() {
	if w.writeOptions.isDebug {
		w.writeOptions.logger.Logf("flush buffer: %s", w.view.Name)
	}
	w.clickhouseCh <- NewBatch(w.bufferEngine.Read())
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
//}

// writing to a temporary buffer to collect more data
func (w *writer) runBufferBridge() {
	ticker := time.NewTicker(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)
	defer func() {
		ticker.Stop()
		// flush last data
		if w.bufferEngine.Len() > 0 {
			w.flush()
		}
		// close buffer channel
		close(w.bufferCh)
		w.bufferCh = nil
		// send signal, buffer listener is done
		w.doneCh <- struct{}{}
		w.writeOptions.logger.Logf("stop buffer bridge: %s", w.view.Name)
	}()
	w.writeOptions.logger.Logf("run buffer bridge: %s", w.view.Name)
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

// asynchronously write to Clickhouse database in large batches
func (w *writer) runClickhouseBridge() {
	w.writeOptions.logger.Logf("run clickhouse bridge: %s", w.view.Name)
	defer func() {
		// close clickhouse channel
		close(w.clickhouseCh)
		w.clickhouseCh = nil
		// close errors channel if it created
		w.mu.Lock()
		if w.errCh != nil {
			close(w.errCh)
			w.errCh = nil
		}
		w.mu.Unlock()
		// send signal, clickhouse listener is done
		w.doneCh <- struct{}{}
		w.writeOptions.logger.Logf("stop clickhouse bridge: %s", w.view.Name)
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

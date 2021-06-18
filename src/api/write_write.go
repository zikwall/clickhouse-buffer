package api

import "time"

// Errors returns a channel for reading errors which occurs during async writes.
// Must be called before performing any writes for errors to be collected.
// The chan is unbuffered and must be drained or the writer will block.
func (w *writerImpl) Errors() <-chan error {
	if w.errCh == nil {
		w.errCh = make(chan error)
	}
	return w.errCh
}

// Flush forces all pending writes from the buffer to be sent
func (w *writerImpl) Flush() {
	w.bufferFlush <- struct{}{}
	w.awaitFlushing()
}

func (w *writerImpl) awaitFlushing() {
	// waiting buffer is flushed
	<-time.After(time.Millisecond)
}

// Close finishes outstanding write operations,
// stop background routines and closes all channels
func (w *writerImpl) Close() {
	if w.writeCh != nil {
		// Flush outstanding metrics
		w.Flush()

		// stop and wait for buffer proc
		close(w.bufferStop)
		<-w.doneCh

		close(w.bufferFlush)
		close(w.bufferCh)

		// stop and wait for write proc
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
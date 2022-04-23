package clickhousebuffer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zikwall/clickhouse-buffer/src/buffer"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
)

const (
	defaultRetryChanSize = 100
	defaultCycloCount    = 2
)

const (
	defaultAttemptLimit = 3
	defaultFactor       = 100 * time.Millisecond
)

const (
	successfully        = "(DEBUG): SUCCESSFULLY handle retry"
	successfullyAttempt = "(DEBUG): SUCCESSFULLY handle records. Count attempts: %d, count affected: %d"
	attemptError        = "(DEBUG WARNING): attempt error"
	limitOfRetries      = "(WARNING): limit of retries has been reached"
	queueIsFull         = "(ERROR): queue for repeating messages is full..."
)

type imMemoryRetry struct {
	logger       Logger
	writer       Writeable
	retries      chan *retryPacket
	isDebug      bool
	limit        strategy.Strategy
	backoff      strategy.Strategy
	successfully uint64
	failed       uint64
	progress     uint64
}

func NewRetry(ctx context.Context, writer Writeable, logger Logger, isDebug bool) Retryable {
	r := &imMemoryRetry{
		retries: make(chan *retryPacket, defaultRetryChanSize),
		writer:  writer,
		isDebug: isDebug,
		logger:  logger,
		limit:   strategy.Limit(defaultAttemptLimit),
		backoff: strategy.Backoff(backoff.Fibonacci(defaultFactor)),
	}
	go r.backoffRetry(ctx)
	return r
}

func (r *imMemoryRetry) Queue(packet *retryPacket) {
	if value := atomic.AddUint64(&r.progress, 1); value >= defaultRetryChanSize {
		r.logger.Log(queueIsFull)
		return
	}
	r.retries <- packet
}

func (r *imMemoryRetry) Metrics() (successfully, failed, progress uint64) {
	return atomic.LoadUint64(&r.successfully), atomic.LoadUint64(&r.failed), atomic.LoadUint64(&r.progress)
}

func (r *imMemoryRetry) action(ctx context.Context, view View, btc *buffer.Batch) retry.Action {
	return func(attempt uint) error {
		affected, err := r.writer.Write(ctx, view, btc)
		if err != nil {
			if r.isDebug {
				r.logger.Logf("%s: %s", attemptError, err.Error())
			}
			return err
		}
		if r.isDebug {
			r.logger.Logf(successfullyAttempt, attempt, affected)
		}
		return nil
	}
}

// nolint:lll // it's not important here
const (
	runListenerMsg  = "(DEBUG): run im-memory retries listener"
	stopListenerMsg = "(DEBUG): stop im-memory retries listener"
	handleRetryMsg  = "(DEBUG): receive retry message, handle retry packet..."
	packetIsLost    = "(ERROR): packet couldn't be processed within %d retry cycles, packet was removed from queue..."
	packetResend    = "(DEBUG): packet will be sent for resend, cycles left %d"
)

func (r *imMemoryRetry) backoffRetry(ctx context.Context) {
	if r.isDebug {
		r.logger.Log(runListenerMsg)
	}
	defer func() {
		if r.isDebug {
			r.logger.Log(stopListenerMsg)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-r.retries:
			atomic.AddUint64(&r.progress, ^uint64(0))
			if r.isDebug {
				r.logger.Log(handleRetryMsg)
			}
			if err := retry.Retry(r.action(ctx, packet.view, packet.btc), r.limit, r.backoff); err != nil {
				r.logger.Logf("%s: %v", limitOfRetries, err)
				// if error is not in list of not allowed,
				// and the number of repetition cycles has not been exhausted,
				// try to re-send it to the processing queue
				if (packet.tryCount < defaultCycloCount) && isResendAvailable(err) {
					r.Queue(&retryPacket{
						view:     packet.view,
						btc:      packet.btc,
						tryCount: packet.tryCount + 1,
					})
					if r.isDebug {
						r.logger.Logf(packetResend, defaultCycloCount-packet.tryCount-1)
					}
					continue
				}
				// otherwise, increase failed counter and report in logs that the package is always lost
				atomic.AddUint64(&r.failed, 1)
				r.logger.Logf(packetIsLost, defaultCycloCount)
				continue
			}
			// mark packet as successfully processed
			atomic.AddUint64(&r.successfully, 1)
			if r.isDebug {
				r.logger.Log(successfully)
			}
		}
	}
}

package clickhousebuffer

import (
	"context"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"

	"github.com/zikwall/clickhouse-buffer/v2/src/buffer"
	"github.com/zikwall/clickhouse-buffer/v2/src/database"
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

// nolint:lll // it's not important here
const (
	runListenerMsg  = "(DEBUG): worker has been started listening for data resending operations"
	stopListenerMsg = "(DEBUG): worker has been stopped listening for data resending operations"
	handleRetryMsg  = "(DEBUG): receive retry message, handle retry packet..."
	packetIsLost    = "(ERROR): packet couldn't be processed within %d retry cycles, packet was removed from queue..."
	packetResend    = "(DEBUG): packet will be sent for resend, cycles left %d"
)

type Retryable interface {
	Retry(packet *retryPacket)
	Metrics() (uint64, uint64, uint64)
}

type Queueable interface {
	Queue(packet *retryPacket)
	Retries() <-chan *retryPacket
}

type Closable interface {
	Close() error
	CloseMessage() string
}

type retryPacket struct {
	view     database.View
	btc      *buffer.Batch
	tryCount uint8
}

type retryImpl struct {
	logger       Logger
	writer       Writeable
	engine       Queueable
	isDebug      bool
	limit        strategy.Strategy
	backoff      strategy.Strategy
	successfully Countable
	failed       Countable
	progress     Countable
}

func NewRetry(ctx context.Context, engine Queueable, writer Writeable, logger Logger, isDebug bool) Retryable {
	r := &retryImpl{
		engine:       engine,
		writer:       writer,
		logger:       logger,
		isDebug:      isDebug,
		limit:        strategy.Limit(defaultAttemptLimit),
		backoff:      strategy.Backoff(backoff.Fibonacci(defaultFactor)),
		successfully: newUint64Counter(),
		failed:       newUint64Counter(),
		progress:     newUint64Counter(),
	}
	go r.backoffRetry(ctx)
	return r
}

func (r *retryImpl) Metrics() (successfully, failed, progress uint64) {
	return r.successfully.Val(), r.failed.Val(), r.progress.Val()
}

func (r *retryImpl) Retry(packet *retryPacket) {
	if value := r.progress.Inc(); value >= defaultRetryChanSize {
		r.logger.Log(queueIsFull)
		return
	}
	r.engine.Queue(packet)
}

func (r *retryImpl) backoffRetry(ctx context.Context) {
	if r.isDebug {
		r.logger.Log(runListenerMsg)
	}
	defer func() {
		if closable, ok := r.engine.(Closable); ok {
			r.logger.Log(closable.CloseMessage())
			if err := closable.Close(); err != nil {
				r.logger.Log(err)
			}
		}
		if r.isDebug {
			r.logger.Log(stopListenerMsg)
		}
	}()
	retries := r.engine.Retries()
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-retries:
			r.handlePacket(ctx, packet)
		}
	}
}

func (r *retryImpl) action(ctx context.Context, view database.View, btc *buffer.Batch) retry.Action {
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

// if error is not in list of not allowed,
// and the number of repetition cycles has not been exhausted,
// try to re-send it to the processing queue
func (r *retryImpl) resend(packet *retryPacket, err error) bool {
	if (packet.tryCount < defaultCycloCount) && database.IsResendAvailable(err) {
		r.Retry(&retryPacket{
			view:     packet.view,
			btc:      packet.btc,
			tryCount: packet.tryCount + 1,
		})
		if r.isDebug {
			r.logger.Logf(packetResend, defaultCycloCount-packet.tryCount-1)
		}
		return true
	}
	return false
}

func (r *retryImpl) handlePacket(ctx context.Context, packet *retryPacket) {
	r.progress.Dec()
	if r.isDebug {
		r.logger.Log(handleRetryMsg)
	}
	if err := retry.Retry(r.action(ctx, packet.view, packet.btc), r.limit, r.backoff); err != nil {
		r.logger.Logf("%s: %v", limitOfRetries, err)
		if !r.resend(packet, err) {
			// otherwise, increase failed counter and report in logs that the package is always lost
			r.failed.Inc()
			r.logger.Logf(packetIsLost, defaultCycloCount)
		}
	} else {
		// mark packet as successfully processed
		r.successfully.Inc()
		if r.isDebug {
			r.logger.Log(successfully)
		}
	}
}

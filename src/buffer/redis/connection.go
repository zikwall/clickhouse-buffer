package redis

import (
	"context"
	"errors"

	"github.com/zikwall/clickhouse-buffer/v2/src/buffer"

	"github.com/go-redis/redis/v8"
)

const prefix = "ch_buffer"

func key(bucket string) string {
	return prefix + ":" + bucket
}

type redisBufferImpl struct {
	client     *redis.Client
	context    context.Context
	bucket     string
	bufferSize int64
}

func NewBuffer(ctx context.Context, rdb *redis.Client, bucket string, bufferSize uint) (buffer.Buffer, error) {
	return &redisBufferImpl{
		client:     rdb,
		context:    ctx,
		bucket:     key(bucket),
		bufferSize: int64(bufferSize),
	}, nil
}

func (rb *redisBufferImpl) isContextClosedErr(err error) bool {
	return errors.Is(err, redis.ErrClosed) && rb.context.Err() != nil && rb.context.Err() == context.Canceled
}

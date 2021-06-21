package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

const prefix = "ch_buffer"

func key(bucket string) string {
	return prefix + ":" + bucket
}

type RedisBuffer struct {
	client     *redis.Client
	context    context.Context
	bucket     string
	bufferSize int64
}

func NewRedisBuffer(ctx context.Context, rdb *redis.Client, bucket string, bufferSize uint) (*RedisBuffer, error) {
	return &RedisBuffer{
		client:     rdb,
		context:    ctx,
		bucket:     key(bucket),
		bufferSize: int64(bufferSize),
	}, nil
}

func (rb *RedisBuffer) isShutdownClosedError(err error) bool {
	return errors.Is(err, redis.ErrClosed) && rb.context.Err() != nil && rb.context.Err() == context.Canceled
}

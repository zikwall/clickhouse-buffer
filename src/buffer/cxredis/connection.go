package cxredis

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

const prefix = "ch_buffer"

func key(bucket string) string {
	return prefix + ":" + bucket
}

type redisBuffer struct {
	client     *redis.Client
	context    context.Context
	bucket     string
	bufferSize int64
	size       int64
}

func NewBuffer(ctx context.Context, rdb *redis.Client, bucket string, bufferSize uint) (cx.Buffer, error) {
	return &redisBuffer{
		client:     rdb,
		context:    ctx,
		bucket:     key(bucket),
		bufferSize: int64(bufferSize),
		size:       rdb.LLen(ctx, bucket).Val(),
	}, nil
}

func (r *redisBuffer) isContextClosedErr(err error) bool {
	return errors.Is(err, redis.ErrClosed) && r.context.Err() != nil && r.context.Err() == context.Canceled
}

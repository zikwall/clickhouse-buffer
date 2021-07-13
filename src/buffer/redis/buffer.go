package redis

import (
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"log"
)

func (rb *redisBufferImpl) Write(row buffer.RowSlice) {
	buf, err := row.Encode()
	if err == nil {
		err = rb.client.RPush(rb.context, rb.bucket, buf).Err()
		if err != nil && !rb.isContextClosedErr(err) {
			log.Printf("redis buffer write err: %v\n", err.Error())
		}
	} else {
		log.Printf("redis buffer value encode err: %v\n", err.Error())
	}
}

func (rb *redisBufferImpl) Read() []buffer.RowSlice {
	values := rb.client.LRange(rb.context, rb.bucket, 0, rb.bufferSize).Val()
	slices := make([]buffer.RowSlice, 0, len(values))

	for _, value := range values {
		if v, err := buffer.RowDecoded(value).Decode(); err == nil {
			slices = append(slices, v)
		} else {
			log.Printf("redis buffer read err: %v\n", err.Error())
		}
	}

	return slices
}

func (rb *redisBufferImpl) Len() int {
	return int(rb.client.LLen(rb.context, rb.bucket).Val())
}

func (rb *redisBufferImpl) Flush() {
	rb.client.LTrim(rb.context, rb.bucket, rb.bufferSize, -1).Val()
}

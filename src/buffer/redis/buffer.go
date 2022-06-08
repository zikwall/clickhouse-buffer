package redis

import (
	"log"

	"github.com/zikwall/clickhouse-buffer/v2/src/buffer"
)

func (r *redisBuffer) Write(row buffer.RowSlice) {
	buf, err := row.Encode()
	if err == nil {
		err = r.client.RPush(r.context, r.bucket, buf).Err()
		if err != nil && !r.isContextClosedErr(err) {
			log.Printf("redis buffer write err: %v\n", err.Error())
		}
	} else {
		log.Printf("redis buffer value encode err: %v\n", err.Error())
	}
}

func (r *redisBuffer) Read() []buffer.RowSlice {
	values := r.client.LRange(r.context, r.bucket, 0, r.bufferSize).Val()
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

func (r *redisBuffer) Len() int {
	return int(r.client.LLen(r.context, r.bucket).Val())
}

func (r *redisBuffer) Flush() {
	r.client.LTrim(r.context, r.bucket, r.bufferSize, -1).Val()
}

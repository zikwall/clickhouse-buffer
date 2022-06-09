package cxredis

import (
	"log"

	"github.com/zikwall/clickhouse-buffer/v2/src/cx"
)

func (r *redisBuffer) Write(row cx.Vector) {
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

func (r *redisBuffer) Read() []cx.Vector {
	values := r.client.LRange(r.context, r.bucket, 0, r.bufferSize).Val()
	slices := make([]cx.Vector, 0, len(values))
	for _, value := range values {
		if v, err := cx.VectorDecoded(value).Decode(); err == nil {
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

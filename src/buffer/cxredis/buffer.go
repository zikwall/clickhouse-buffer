package cxredis

import (
	"log"
	"sync/atomic"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

func (r *redisBuffer) Write(row cx.Vector) {
	var err error
	var buf []byte
	if buf, err = row.Encode(); err != nil {
		log.Printf("redis buffer value encode err: %v\n", err.Error())
		return
	}
	if err = r.client.RPush(r.context, r.bucket, buf).Err(); err != nil {
		if !r.isContextClosedErr(err) {
			log.Printf("redis buffer write err: %v\n", err.Error())
		}
		return
	}
	atomic.AddInt64(&r.size, 1)
}

func (r *redisBuffer) Read() []cx.Vector {
	values := r.client.LRange(r.context, r.bucket, 0, atomic.LoadInt64(&r.size)).Val()
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
	return int(atomic.LoadInt64(&r.size))
}

func (r *redisBuffer) Flush() {
	r.client.LTrim(r.context, r.bucket, r.bufferSize, -1).Val()
	atomic.StoreInt64(&r.size, 0)
}

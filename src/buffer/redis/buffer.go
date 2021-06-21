package redis

import (
	"encoding/json"
	"github.com/zikwall/clickhouse-buffer/src/types"
	"log"
)

func (rb *RedisBuffer) Write(row types.RowSlice) {
	value, err := json.Marshal(row)
	if err != nil {
		log.Printf("redis buffer write err: %v\n", err.Error())
		return
	}

	err = rb.client.RPush(rb.context, rb.bucket, string(value)).Err()
	if !rb.isContextClosedErr(err) {
		log.Printf("redis buffer write err: %v\n", err.Error())
	}
}

func (rb *RedisBuffer) Read() []types.RowSlice {
	values := rb.client.LRange(rb.context, rb.bucket, 0, rb.bufferSize).Val()
	slices := make([]types.RowSlice, 0, len(values))

	var v types.RowSlice
	for _, value := range values {
		if err := json.Unmarshal([]byte(value), &v); err == nil {
			slices = append(slices, v)
		} else {
			log.Printf("redis buffer read err: %v\n", err.Error())
		}
	}

	return slices
}

func (rb *RedisBuffer) Len() int {
	return int(rb.client.LLen(rb.context, rb.bucket).Val())
}

func (rb *RedisBuffer) Flush() {
	rb.client.LTrim(rb.context, rb.bucket, 0, rb.bufferSize).Val()
}

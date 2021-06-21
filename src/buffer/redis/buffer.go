package redis

import (
	"encoding/json"
	"github.com/zikwall/clickhouse-buffer/src/types"
	"log"
)

func (rb *RedisBuffer) Write(row types.RowSlice) {
	value, err := json.Marshal(row)

	if err != nil {
		log.Println(err)
		return
	}

	err = rb.client.RPush(rb.context, rb.bucket, string(value)).Err()

	if !rb.isShutdownClosedError(err) {
		log.Println(err)
	}
}

func (rb *RedisBuffer) Read() []types.RowSlice {
	values := rb.client.LRange(rb.context, rb.bucket, 0, rb.bufferSize).Val()
	slices := make([]types.RowSlice, 0, len(values))

	for _, value := range values {
		var v types.RowSlice
		if err := json.Unmarshal([]byte(value), &v); err == nil {
			slices = append(slices, v)
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

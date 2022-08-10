package bench

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/go-redis/redis/v8"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v3"
	"github.com/zikwall/clickhouse-buffer/v3/example/pkg/tables"
	"github.com/zikwall/clickhouse-buffer/v3/src/buffer/cxredis"
	"github.com/zikwall/clickhouse-buffer/v3/src/cx"
)

// x100
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertRedisObjects/1000-12                      100          22404356 ns/op           96095 B/op       2322 allocs/op
// BenchmarkInsertRedisObjects/100-12                       100           2243544 ns/op            9673 B/op        233 allocs/op
// BenchmarkInsertRedisObjects/10-12                        100            271749 ns/op            1033 B/op         25 allocs/op
// PASS
// ok
// nolint:funlen,dupl // it's not important here
func BenchmarkInsertRedisObjects(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000).
			SetBatchSize(1000),
	)
	redisHost := os.Getenv("REDIS_HOST")
	redisPass := os.Getenv("REDIS_PASS")
	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 100, b)
		}
	})

	b.Run("10", func(b *testing.B) {
		client.Options().SetBatchSize(11)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 10, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// x100
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertRedisVectors/1000-12                  100          22145258 ns/op           92766 B/op       2274 allocs/op
// BenchmarkInsertRedisVectors/100-12                   100           2320692 ns/op            9339 B/op        229 allocs/op
// BenchmarkInsertRedisVectors/10-12                    100            202146 ns/op             157 B/op          2 allocs/op
// PASS
// ok
// nolint:funlen,dupl // it's not important here
func BenchmarkInsertRedisVectors(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000).
			SetBatchSize(1000),
	)
	redisHost := os.Getenv("REDIS_HOST")
	redisPass := os.Getenv("REDIS_PASS")
	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("1000", func(b *testing.B) {
		b.StopTimer()
		client.Options().SetBatchSize(1001)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		b.StopTimer()
		client.Options().SetBatchSize(101)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 100, b)
		}
	})

	b.Run("10", func(b *testing.B) {
		b.StopTimer()
		client.Options().SetBatchSize(11)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 10, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

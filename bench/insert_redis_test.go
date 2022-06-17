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

// x10
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertRedisObjects/10000-12                  10        2165062600 ns/op         8949147 B/op     215127 allocs/op
// BenchmarkInsertRedisObjects/1000-12                   10         205537440 ns/op          960664 B/op      23209 allocs/op
// BenchmarkInsertRedisObjects/100-12                    10          20371570 ns/op           96292 B/op       2323 allocs/op
// BenchmarkInsertRedisObjects/10-12                     10           2216160 ns/op            1868 B/op         32 allocs/op
// BenchmarkInsertRedisObjects/1-12                      10            180490 ns/op             188 B/op          3 allocs/op
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

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10000)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
			DB:       11,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1000)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(100)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 100, b)
		}
	})

	b.Run("10", func(b *testing.B) {
		client.Options().SetBatchSize(10)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 10, b)
		}
	})

	b.Run("1", func(b *testing.B) {
		client.Options().SetBatchSize(1)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// x10
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertRedisVectors/10000-12                  10        2035777840 ns/op         9124870 B/op     223549 allocs/op
// BenchmarkInsertRedisVectors/1000-12                   10         208882330 ns/op          926325 B/op      22709 allocs/op
// BenchmarkInsertRedisVectors/100-12                    10          19944150 ns/op           92810 B/op       2273 allocs/op
// BenchmarkInsertRedisVectors/10-12                     10           1891890 ns/op            1588 B/op         28 allocs/op
// BenchmarkInsertRedisVectors/1-12                      10            182180 ns/op             160 B/op          2 allocs/op
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

	b.Run("10000", func(b *testing.B) {
		b.StopTimer()
		client.Options().SetBatchSize(10000)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
			DB:       11,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		b.StopTimer()
		client.Options().SetBatchSize(1000)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
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
		client.Options().SetBatchSize(100)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
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
		client.Options().SetBatchSize(10)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 10, b)
		}
	})

	b.Run("1", func(b *testing.B) {
		b.StopTimer()
		client.Options().SetBatchSize(1)
		rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
			Addr:     redisHost,
			Password: redisPass,
		}), "bucket", client.Options().BatchSize())
		if err != nil {
			log.Panicln(err)
		}
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			rxbuffer,
		)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 1, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

package bench

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-redis/redis/v8"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v3"
	"github.com/zikwall/clickhouse-buffer/v3/example/pkg/tables"
	"github.com/zikwall/clickhouse-buffer/v3/src/buffer/cxmem"
	"github.com/zikwall/clickhouse-buffer/v3/src/buffer/cxredis"
	"github.com/zikwall/clickhouse-buffer/v3/src/cx"
)

type BenchTable struct {
	ID       int32
	UUID     string
	InsertTS time.Time
}

func (t *BenchTable) Row() cx.Vector {
	return cx.Vector{t.ID, t.UUID, t.InsertTS.Format(time.RFC822)}
}

type clickhouseMock struct{}

func (c *clickhouseMock) Insert(_ context.Context, _ cx.View, _ []cx.Vector) (uint64, error) {
	return 0, nil
}

func (c *clickhouseMock) Close() error {
	return nil
}

func (c *clickhouseMock) Conn() driver.Conn {
	return nil
}

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
			insertVectors(writeAPI, 10000, b)
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
			insertVectors(writeAPI, 1000, b)
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
			insertVectors(writeAPI, 100, b)
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
			insertVectors(writeAPI, 10, b)
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
			insertVectors(writeAPI, 1, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// x50
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestPreallocateVectors/10000000-12                 50          25841852 ns/op         4800020 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/1000000-12                  50           2611338 ns/op          480053 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/100000-12                   50            252132 ns/op           48005 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/10000-12                    50             33340 ns/op            4915 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/1000-12                     50              3496 ns/op             492 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/100-12                      50               102.0 ns/op            54 B/op         0 allocs/op
// PASS
// ok
// nolint:lll,dupl // it's OK
func BenchmarkInsertSimplestPreallocateVectors(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000000).
			SetBatchSize(10000000),
	)

	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("10000000", func(b *testing.B) {
		client.Options().SetBatchSize(10000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 10000000, b)
		}
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(100)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 100, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// x50
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestPreallocateObjects/10000000-12                 50          80617426 ns/op        22400022 B/op    600000 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/1000000-12                  50           7746850 ns/op         2240051 B/op     60000 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/100000-12                   50            900776 ns/op          224005 B/op      6000 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/10000-12                    50             75438 ns/op           22515 B/op       600 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/1000-12                     50              7454 ns/op            2252 B/op        60 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/100-12                      50               618.0 ns/op           230 B/op         6 allocs/op
// PASS
// ok
// nolint:lll,dupl // it's OK
func BenchmarkInsertSimplestPreallocateObjects(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000000).
			SetBatchSize(10000000),
	)

	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("10000000", func(b *testing.B) {
		client.Options().SetBatchSize(10000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 10000000, b)
		}
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(100)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 100, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// x50
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestObjects/10000000-12                    50          88113322 ns/op        32000020 B/op     800000 allocs/op
// BenchmarkInsertSimplestObjects/1000000-12                     50           8273780 ns/op         3200052 B/op      80000 allocs/op
// BenchmarkInsertSimplestObjects/100000-12                      50            891610 ns/op          320005 B/op       8000 allocs/op
// BenchmarkInsertSimplestObjects/10000-12                       50             83918 ns/op           27200 B/op        800 allocs/op
// BenchmarkInsertSimplestObjects/1000-12                        50              8258 ns/op            3212 B/op         80 allocs/op
// BenchmarkInsertSimplestObjects/100-12                         50               818.0 ns/op           326 B/op          8 allocs/op
// PASS
// ok
// nolint:lll,dupl // it's OK
func BenchmarkInsertSimplestObjects(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000000).
			SetBatchSize(10000000),
	)

	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("10000000", func(b *testing.B) {
		client.Options().SetBatchSize(10000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 10000000, b)
		}
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(100)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 100, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// ~2x total more effective than WriteRow
// x50
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestVectors/10000000-12              50          75695376 ns/op        14400020 B/op     200000 allocs/op
// BenchmarkInsertSimplestVectors/1000000-12               50           5001434 ns/op         1440051 B/op      20000 allocs/op
// BenchmarkInsertSimplestVectors/100000-12                50            403230 ns/op          144005 B/op       2000 allocs/op
// BenchmarkInsertSimplestVectors/10000-12                 50             33936 ns/op           14515 B/op        200 allocs/op
// BenchmarkInsertSimplestVectors/1000-12                  50              3140 ns/op            1452 B/op         20 allocs/op
// BenchmarkInsertSimplestVectors/100-12                   50               186.0 ns/op           150 B/op          2 allocs/op
// PASS
// ok
// nolint:lll,dupl // it's OK
func BenchmarkInsertSimplestVectors(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000000).
			SetBatchSize(100),
	)

	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("10000000", func(b *testing.B) {
		client.Options().SetBatchSize(10000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 10000000, b)
		}
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(100)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 100, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v3/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestEmptyVectors/10000000-12                      100          22317607 ns/op               0 B/op         0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/1000000-12                       100           1246957 ns/op          240026 B/op         0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/100000-12                        100            125928 ns/op           24002 B/op         0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/10000-12                         100             13788 ns/op            2457 B/op         0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/1000-12                          100              1475 ns/op             246 B/op         0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/100-12                           100               282.0 ns/op            54 B/op         0 allocs/op
// PASS
// ok
// nolint:lll,dupl // it's OK
func BenchmarkInsertSimplestEmptyVectors(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := clickhousebuffer.NewClientWithOptions(ctx, &clickhouseMock{},
		clickhousebuffer.DefaultOptions().
			SetDebugMode(false).
			SetFlushInterval(10000000).
			SetBatchSize(100),
	)

	var writeAPI clickhousebuffer.Writer
	b.ResetTimer()

	b.Run("10000000", func(b *testing.B) {
		client.Options().SetBatchSize(10000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 10000000, b)
		}
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1000)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(100)
		writeAPI = client.Writer(
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 100, b)
		}
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

func insertObjects(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	b.ResetTimer()
	var object *BenchTable
	for i := 0; i < x; i++ {
		object = &BenchTable{ID: 1}
		writeAPI.WriteRow(object)
	}
}

func insertVectors(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	b.ResetTimer()
	var vector cx.Vector
	for i := 0; i < x; i++ {
		vector = cx.Vector{1, "", ""}
		writeAPI.WriteVector(vector)
	}
}

func insertEmptyVectors(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < x; i++ {
		writeAPI.WriteVector(cx.Vector{})
	}
}

func insertPreAllocatedObjects(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	objects := make([]cx.Vectorable, 0, x+1)
	for i := 0; i < x; i++ {
		objects = append(objects, &BenchTable{ID: 1})
	}
	b.ResetTimer()
	for i := range objects {
		writeAPI.WriteRow(objects[i])
	}
}

func insertPreAllocatedVectors(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	vectors := make([]cx.Vector, 0, x+1)
	for i := 0; i < x; i++ {
		vectors = append(vectors, cx.Vector{1, "", ""})
	}
	b.ResetTimer()
	for i := range vectors {
		writeAPI.WriteVector(vectors[i])
	}
}

package bench

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v4"
	"github.com/zikwall/clickhouse-buffer/v4/example/pkg/tables"
	"github.com/zikwall/clickhouse-buffer/v4/src/buffer/cxmem"
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
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

// x50
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestPreallocateVectors/1000000-12                1000            142919 ns/op               0 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/100000-12                 1000             12498 ns/op               0 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/10000-12                  1000              1265 ns/op               0 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/1000-12                   1000               143.1 ns/op             0 B/op         0 allocs/op
// BenchmarkInsertSimplestPreallocateVectors/100-12                    1000                 5.700 ns/op           2 B/op         0 allocs/op
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

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
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

// x1000
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestPreallocateObjects/1000000-12                1000            399110 ns/op           88000 B/op      3000 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/100000-12                 1000             37527 ns/op            8800 B/op       300 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/10000-12                  1000              3880 ns/op             880 B/op        30 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/1000-12                   1000               419.5 ns/op            88 B/op         3 allocs/op
// BenchmarkInsertSimplestPreallocateObjects/100-12                    1000                58.90 ns/op           11 B/op         0 allocs/op
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

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertPreAllocatedObjects(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
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

// x1000
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestObjects/1000000-12                   1000            454794 ns/op          160002 B/op       4000 allocs/op
// BenchmarkInsertSimplestObjects/100000-12                    1000             41879 ns/op           16000 B/op        400 allocs/op
// BenchmarkInsertSimplestObjects/10000-12                     1000              4174 ns/op            1605 B/op         40 allocs/op
// BenchmarkInsertSimplestObjects/1000-12                      1000               479.5 ns/op           160 B/op          4 allocs/op
// BenchmarkInsertSimplestObjects/100-12                       1000                39.40 ns/op           16 B/op          0 allocs/op
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

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertObjects(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
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

// X1000
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestObjectsJust/10000000-12              1000           4705290 ns/op         1360000 B/op      40000 allocs/op
// BenchmarkInsertSimplestObjectsJust/1000000-12               1000            410051 ns/op          136000 B/op       4000 allocs/op
// BenchmarkInsertSimplestObjectsJust/100000-12                1000             45773 ns/op           13600 B/op        400 allocs/op
// BenchmarkInsertSimplestObjectsJust/10000-12                 1000              4851 ns/op            1360 B/op         40 allocs/op
// BenchmarkInsertSimplestObjectsJust/1000-12                  1000               431.4 ns/op           136 B/op          4 allocs/op
// BenchmarkInsertSimplestObjectsJust/100-12                   1000                66.40 ns/op           13 B/op          0 allocs/op
// PASS
// ok
func BenchmarkInsertSimplestObjectsJust(b *testing.B) {
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
		client.Options().SetBatchSize(10000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertObjects(writeAPI, 10000000, b)
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertObjects(writeAPI, 1000000, b)
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertObjects(writeAPI, 100000, b)
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertObjects(writeAPI, 10000, b)
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertObjects(writeAPI, 1000, b)
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertObjects(writeAPI, 100, b)
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// x1000
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestVectors/1000000-12                   1000            182548 ns/op           72002 B/op       1000 allocs/op
// BenchmarkInsertSimplestVectors/100000-12                    1000             16291 ns/op            7200 B/op        100 allocs/op
// BenchmarkInsertSimplestVectors/10000-12                     1000              1638 ns/op             725 B/op         10 allocs/op
// BenchmarkInsertSimplestVectors/1000-12                      1000               208.4 ns/op            72 B/op          1 allocs/op
// BenchmarkInsertSimplestVectors/100-12                       1000                20.00 ns/op            7 B/op          0 allocs/op
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

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
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

// X1000
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestVectorsJust/10000000-12              1000           2059182 ns/op          480000 B/op      10000 allocs/op
// BenchmarkInsertSimplestVectorsJust/1000000-12               1000            176129 ns/op           48000 B/op       1000 allocs/op
// BenchmarkInsertSimplestVectorsJust/100000-12                1000             17398 ns/op            4800 B/op        100 allocs/op
// BenchmarkInsertSimplestVectorsJust/10000-12                 1000              1937 ns/op             480 B/op         10 allocs/op
// BenchmarkInsertSimplestVectorsJust/1000-12                  1000               243.9 ns/op            48 B/op          1 allocs/op
// BenchmarkInsertSimplestVectorsJust/100-12                   1000                10.50 ns/op            4 B/op          0 allocs/op
// PASS
// ok
func BenchmarkInsertSimplestVectorsJust(b *testing.B) {
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
		client.Options().SetBatchSize(10000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertVectors(writeAPI, 10000000, b)
	})

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertVectors(writeAPI, 1000000, b)
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertVectors(writeAPI, 100000, b)
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertVectors(writeAPI, 10000, b)
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertVectors(writeAPI, 1000, b)
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		insertVectors(writeAPI, 100, b)
	})

	b.StopTimer()
	writeAPI.Close()
	client.Close()
}

// X1000
// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkInsertSimplestEmptyVectors/1000000-12              1000            132887 ns/op           24002 B/op          0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/100000-12               1000             13404 ns/op            2400 B/op          0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/10000-12                1000              1299 ns/op             245 B/op          0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/1000-12                 1000               122.1 ns/op             0 B/op          0 allocs/op
// BenchmarkInsertSimplestEmptyVectors/100-12                  1000                 6.800 ns/op           0 B/op          0 allocs/op
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

	b.Run("1000000", func(b *testing.B) {
		client.Options().SetBatchSize(1000001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 1000000, b)
		}
	})

	b.Run("100000", func(b *testing.B) {
		client.Options().SetBatchSize(100001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 100000, b)
		}
	})

	b.Run("10000", func(b *testing.B) {
		client.Options().SetBatchSize(10001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 10000, b)
		}
	})

	b.Run("1000", func(b *testing.B) {
		client.Options().SetBatchSize(1001)
		writeAPI = client.Writer(
			ctx,
			cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
			cxmem.NewBuffer(client.Options().BatchSize()),
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertEmptyVectors(writeAPI, 1000, b)
		}
	})

	b.Run("100", func(b *testing.B) {
		client.Options().SetBatchSize(101)
		writeAPI = client.Writer(
			ctx,
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
	// for flush data
	b.StopTimer()
	writeAPI.WriteVector(cx.Vector{})
	b.StartTimer()
}

func insertVectors(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	b.ResetTimer()
	var vector cx.Vector
	for i := 0; i < x; i++ {
		vector = cx.Vector{1, "", ""}
		writeAPI.WriteVector(vector)
	}
	// for flush data
	b.StopTimer()
	writeAPI.WriteVector(cx.Vector{})
	b.StartTimer()
}

func insertEmptyVectors(writeAPI clickhousebuffer.Writer, x int, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < x; i++ {
		writeAPI.WriteVector(cx.Vector{})
	}
	// for flush data
	b.StopTimer()
	writeAPI.WriteVector(cx.Vector{})
	b.StartTimer()
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
	// for flush data
	b.StopTimer()
	// nolint:staticcheck // it's OK
	objects = objects[:0]
	objects = nil
	writeAPI.WriteVector(cx.Vector{})
	b.StartTimer()
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
	// for flush data
	b.StopTimer()
	// nolint:staticcheck // it's OK
	vectors = vectors[:0]
	vectors = nil
	writeAPI.WriteVector(cx.Vector{})
	b.StartTimer()
}

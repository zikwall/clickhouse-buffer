[![build](https://github.com/zikwall/clickhouse-buffer/workflows/build_and_tests/badge.svg)](https://github.com/zikwall/clickhouse-buffer/v3/actions)
[![build](https://github.com/zikwall/clickhouse-buffer/workflows/golangci_lint/badge.svg)](https://github.com/zikwall/clickhouse-buffer/v3/actions)

<div align="center">
  <h1>Clickhouse Buffer</h1>
  <h5>An easy-to-use, powerful and productive package for writing data to Clickhouse columnar database</h5>
</div>

## Install

- for go-clickhouse v1 `$ go get -u github.com/zikwall/clickhouse-buffer`
- for go-clickhouse v2 `$ go get -u github.com/zikwall/clickhouse-buffer/v4`

### Why and why

In the practice of using the Clickhouse database (in real projects), 
you often have to resort to creating your own ~~bicycles~~ in the form of queues 
and testers that accumulate the necessary amount of data or for a certain period of time 
and send one large data package to the Clickhouse database.

This is due to the fact that Clickhouse is designed so that it better processes new data in batches 
(and this is recommended by the authors themselves).

### Features

- [x] **non-blocking** - (recommend) async write client uses implicit batching.
  Data are asynchronously written to the underlying buffer and they are automatically sent to a server
  when the size of the write buffer reaches the batch size, default 5000, or the flush interval,
  default 1s, times out. Asynchronous write client is recommended for frequent periodic writes.
- [x] **blocking.**

**Client buffer engines:**

- [x] **in-memory** - use native channels and slices
- [x] **redis** - use redis server as queue and buffer
- [x] **in-memory-sync** - if you get direct access to buffer, it will help to avoid data race
- [x] **retries** - resending "broken" or for some reason not sent packets

### Usage

```go
import (
    "database/sql"

    "github.com/zikwall/clickhouse-buffer/v3/src/cx"
    "github.com/zikwall/clickhouse-buffer/v3/src/db/cxnative"
    "github.com/zikwall/clickhouse-buffer/v3/src/db/cxsql"
)

// if you already have a connection to Clickhouse you can just use wrappers
// with native interface
ch := cxnative.NewClickhouseWithConn(driver.Conn, &cx.RuntimeOptions{})
// or use database/sql interface
ch := cxsql.NewClickhouseWithConn(*sql.DB, &cx.RuntimeOptions{})
```

```go
// if you don't want to create connections yourself, 
// package can do it for you, just call the connection option you need:

// with native interface
ch, conn, err := cxnative.NewClickhouse(ctx, &clickhouse.Options{
        Addr: ctx.StringSlice("clickhouse-host"),
        Auth: clickhouse.Auth{
            Database:  ctx.String("clickhouse-database"),
            Username:  ctx.String("clickhouse-username"),
            Password:  ctx.String("clickhouse-password"),
        },
        Settings: clickhouse.Settings{
            "max_execution_time": 60,
        },
        DialTimeout: 5 * time.Second,
        Compression: &clickhouse.Compression{
            Method: clickhouse.CompressionLZ4,
        },
        Debug: ctx.Bool("debug"),
}, &cx.RuntimeOptions{})
// or with database/sql interface
ch, conn, err := cxsql.NewClickhouse(ctx, &clickhouse.Options{
        Addr: ctx.StringSlice("clickhouse-host"),
        Auth: clickhouse.Auth{
            Database:  ctx.String("clickhouse-database"),
            Username:  ctx.String("clickhouse-username"),
            Password:  ctx.String("clickhouse-password"),
        },
        Settings: clickhouse.Settings{
            "max_execution_time": 60,
        },
        DialTimeout: 5 * time.Second,
        Compression: &clickhouse.Compression{
            Method: clickhouse.CompressionLZ4,
        },
        Debug: ctx.Bool("debug"),
}, &cx.RuntimeOptions{})
```

#### Create main data streamer client and write data

```go
import (
    clickhousebuffer "github.com/zikwall/clickhouse-buffer/v3"
    "github.com/zikwall/clickhouse-buffer/v3/src/buffer/cxmem"
    "github.com/zikwall/clickhouse-buffer/v3/src/db/cxnative"
)
// create root client
client := clickhousebuffer.NewClientWithOptions(ctx, ch,
    clickhousebuffer.DefaultOptions().SetFlushInterval(1000).SetBatchSize(5000),
)
// create buffer engine
buffer := cxmem.NewBuffer(
    client.Options().BatchSize(),
)
// or use redis
buffer := cxredis.NewBuffer(
    ctx, *redis.Client, "bucket", client.Options().BatchSize(),
)
// create new writer api: table name with columns
writeAPI := client.Writer(
	ctx,
	cx.NewView("clickhouse_database.clickhouse_table", []string{"id", "uuid", "insert_ts"}), 
	buffer,
)

// define your custom data structure
type MyCustomDataView struct {
	id       int
	uuid     string
	insertTS time.Time
}
// and implement cx.Vectorable interface
func (t *MyCustomDataView) Row() cx.Vector {
	return cx.Vector{t.id, t.uuid, t.insertTS.Format(time.RFC822)}
}
// async write your data
writeAPI.WriteRow(&MyCustomDataView{
    id: 1, uuid: "1", insertTS: time.Now(),
})
// or use a safe way (same as WriteRow, but safer)
writeAPI.TryWriteRow(&MyCustomDataView{
    id: 1, uuid: "1", insertTS: time.Now(),
})
// or faster
writeAPI.WriteVector(cx.Vector{
    1, "1", time.Now(),
})
// safe way
writeAPI.TryWriteVector(cx.Vector{
    1, "1", time.Now(),
})
```

When using a non-blocking record, you can track errors through a special error channel

```go
errorsCh := writeAPI.Errors()
go func() {
	for err := range errorsCh {
		log.Warning(fmt.Sprintf("clickhouse write error: %s", err.Error()))
	}
}()
```

Using the blocking writer interface

```go
// create new writer api: table name with columns
writerBlocking := client.WriterBlocking(cx.View{
    Name:    "clickhouse_database.clickhouse_table",
    Columns: []string{"id", "uuid", "insert_ts"},
})
// non-asynchronous writing of data directly to Clickhouse
err := writerBlocking.WriteRow(ctx, []&MyCustomDataView{
    {
        id: 1, uuid: "1", insertTS: time.Now(),
    },
    {
        id: 2, uuid: "2", insertTS: time.Now(),
    },
    {
        id: 3, uuid: "3", insertTS: time.Now(),
    },
}...)
```

### More

#### Buffer engine:

You can implement own data-buffer interface: `File`, `Rabbitmq`, `CustomMemory`, etc.

```go
type Buffer interface {
	Write(vec Vector)
	Read() []Vector
	Len() int
	Flush()
}
```

#### Retries:

> By default, packet resending is disabled, to enable it, you need to call `(*Options).SetRetryIsEnabled(true)`.

- [x] in-memory use channels (default)
- [ ] redis
- [ ] rabbitMQ
- [ ] kafka

You can implement queue engine by defining the `Queueable` interface:

```go
type Queueable interface {
	Queue(packet *Packet)
	Retries() <-chan *Packet
}
```

and set it as an engine:

```go
clickhousebuffer.DefaultOptions().SetDebugMode(true).SetRetryIsEnabled(true).SetQueueEngine(CustomQueueable)
```

#### Logs:

You can implement your logger by simply implementing the Logger interface and throwing it in options:

```go
type Logger interface {
	Log(message interface{})
	Logf(format string, v ...interface{})
}
```

```go
// example with default options
clickhousebuffer.DefaultOptions().SetDebugMode(true).SetLogger(SomeLogger)
```

#### Tests:

- `$ go test -v ./...` - run all tests without integration part
- `$ go test -race -v ./...` - run all tests without integration part and in race detection mode
- `$ golangci-lint run --config ./.golangci.yml` - check code quality with linters

**Integration Tests:**

```shell
export CLICKHOUSE_HOST=111.11.11.11:9000
export REDIS_HOST=111.11.11.11:6379
export REDIS_PASS=password_if_needed

$ go test -v ./... -tags=integration

or with race detec mode

$ go test -race -v ./... -tags=integration
```

**Benchmarks**

**Ubuntu 20.04/Intel Core i7-8750H**

```shell
goos: linux
goarch: amd64
pkg: github.com/zikwall/clickhouse-buffer/v3/bench
cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
```

```shell
// memory

$ go test ./bench -bench=BenchmarkInsertSimplestPreallocateVectors -benchmem -benchtime=1000x

BenchmarkInsertSimplestPreallocateVectors/1000000-12                1000            142919 ns/op               0 B/op         0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/100000-12                 1000             12498 ns/op               0 B/op         0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/10000-12                  1000              1265 ns/op               0 B/op         0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/1000-12                   1000               143.1 ns/op             0 B/op         0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/100-12                    1000                 5.700 ns/op           2 B/op         0 allocs/op

$ go test ./bench -bench=BenchmarkInsertSimplestPreallocateObjects -benchmem -benchtime=1000x

BenchmarkInsertSimplestPreallocateObjects/1000000-12                1000            399110 ns/op           88000 B/op      3000 allocs/op
BenchmarkInsertSimplestPreallocateObjects/100000-12                 1000             37527 ns/op            8800 B/op       300 allocs/op
BenchmarkInsertSimplestPreallocateObjects/10000-12                  1000              3880 ns/op             880 B/op        30 allocs/op
BenchmarkInsertSimplestPreallocateObjects/1000-12                   1000               419.5 ns/op            88 B/op         3 allocs/op
BenchmarkInsertSimplestPreallocateObjects/100-12                    1000                58.90 ns/op           11 B/op         0 allocs/op

$ go test ./bench -bench=BenchmarkInsertSimplestObjects -benchmem -benchtime=1000x

BenchmarkInsertSimplestObjects/1000000-12                   1000            454794 ns/op          160002 B/op       4000 allocs/op
BenchmarkInsertSimplestObjects/100000-12                    1000             41879 ns/op           16000 B/op        400 allocs/op
BenchmarkInsertSimplestObjects/10000-12                     1000              4174 ns/op            1605 B/op         40 allocs/op
BenchmarkInsertSimplestObjects/1000-12                      1000               479.5 ns/op           160 B/op          4 allocs/op
BenchmarkInsertSimplestObjects/100-12                       1000                39.40 ns/op           16 B/op          0 allocs/op

$ go test ./bench -bench=BenchmarkInsertSimplestVectors -benchmem -benchtime=1000x

BenchmarkInsertSimplestVectors/1000000-12                   1000            182548 ns/op           72002 B/op       1000 allocs/op
BenchmarkInsertSimplestVectors/100000-12                    1000             16291 ns/op            7200 B/op        100 allocs/op
BenchmarkInsertSimplestVectors/10000-12                     1000              1638 ns/op             725 B/op         10 allocs/op
BenchmarkInsertSimplestVectors/1000-12                      1000               208.4 ns/op            72 B/op          1 allocs/op
BenchmarkInsertSimplestVectors/100-12                       1000                20.00 ns/op            7 B/op          0 allocs/op

$ go test ./bench -bench=BenchmarkInsertSimplestEmptyVectors -benchmem -benchtime=1000x

BenchmarkInsertSimplestEmptyVectors/1000000-12              1000            132887 ns/op           24002 B/op          0 allocs/op
BenchmarkInsertSimplestEmptyVectors/100000-12               1000             13404 ns/op            2400 B/op          0 allocs/op
BenchmarkInsertSimplestEmptyVectors/10000-12                1000              1299 ns/op             245 B/op          0 allocs/op
BenchmarkInsertSimplestEmptyVectors/1000-12                 1000               122.1 ns/op             0 B/op          0 allocs/op
BenchmarkInsertSimplestEmptyVectors/100-12                  1000                 6.800 ns/op           0 B/op          0 allocs/op

// redis

$ go test ./bench -bench=BenchmarkInsertRedisObjects -benchmem -benchtime=100x

BenchmarkInsertRedisObjects/1000-12                      100          22404356 ns/op           96095 B/op       2322 allocs/op
BenchmarkInsertRedisObjects/100-12                       100           2243544 ns/op            9673 B/op        233 allocs/op
BenchmarkInsertRedisObjects/10-12                        100            271749 ns/op            1033 B/op         25 allocs/op

$ go test ./bench -bench=BenchmarkInsertRedisVectors -benchmem -benchtime=100x

BenchmarkInsertRedisVectors/1000-12                  100          22145258 ns/op           92766 B/op       2274 allocs/op
BenchmarkInsertRedisVectors/100-12                   100           2320692 ns/op            9339 B/op        229 allocs/op
BenchmarkInsertRedisVectors/10-12                    100            202146 ns/op             157 B/op          2 allocs/op
```

**MacBook Pro M1**

```shell
goos: darwin
goarch: arm64
pkg: github.com/zikwall/clickhouse-buffer/v3/bench
```

```shell
$ akm@MacBook-Pro-andrey clickhouse-buffer % go test ./bench -bench=BenchmarkInsertSimplestPreallocateVectors -benchmem -benchtime=1000x

BenchmarkInsertSimplestPreallocateVectors/1000000-8         	    1000	    206279 ns/op	       0 B/op	       0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/100000-8          	    1000	     24612 ns/op	       0 B/op	       0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/10000-8           	    1000	      2047 ns/op	       0 B/op	       0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/1000-8            	    1000	       204.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkInsertSimplestPreallocateVectors/100-8             	    1000	        22.83 ns/op	       0 B/op	       0 allocs/op

$ akm@MacBook-Pro-andrey clickhouse-buffer % go test ./bench -bench=BenchmarkInsertSimplestPreallocateObjects -benchmem -benchtime=1000x

BenchmarkInsertSimplestPreallocateObjects/1000000-8         	    1000	    410757 ns/op	   88000 B/op	    3000 allocs/op
BenchmarkInsertSimplestPreallocateObjects/100000-8          	    1000	     40885 ns/op	    8800 B/op	     300 allocs/op
BenchmarkInsertSimplestPreallocateObjects/10000-8           	    1000	      4059 ns/op	     880 B/op	      30 allocs/op
BenchmarkInsertSimplestPreallocateObjects/1000-8            	    1000	       407.2 ns/op	      88 B/op	       3 allocs/op
BenchmarkInsertSimplestPreallocateObjects/100-8             	    1000	        46.29 ns/op	      11 B/op	       0 allocs/op

$ akm@MacBook-Pro-andrey clickhouse-buffer % go test ./bench -bench=BenchmarkInsertSimplestObjects -benchmem -benchtime=1000x

BenchmarkInsertSimplestObjects/1000000-8         	    1000	    454083 ns/op	  160002 B/op	    4000 allocs/op
BenchmarkInsertSimplestObjects/100000-8          	    1000	     44329 ns/op	   16000 B/op	     400 allocs/op
BenchmarkInsertSimplestObjects/10000-8           	    1000	      4401 ns/op	    1360 B/op	      40 allocs/op
BenchmarkInsertSimplestObjects/1000-8            	    1000	       437.8 ns/op	     160 B/op	       4 allocs/op
BenchmarkInsertSimplestObjects/100-8             	    1000	        44.71 ns/op	      16 B/op	       0 allocs/op


$ akm@MacBook-Pro-andrey clickhouse-buffer % go test ./bench -bench=BenchmarkInsertSimplestVectors -benchmem -benchtime=1000x

BenchmarkInsertSimplestVectors/1000000-8         	    1000	    244064 ns/op	   72002 B/op	    1000 allocs/op
BenchmarkInsertSimplestVectors/100000-8          	    1000	     24013 ns/op	    7200 B/op	     100 allocs/op
BenchmarkInsertSimplestVectors/10000-8           	    1000	      2335 ns/op	     725 B/op	      10 allocs/op
BenchmarkInsertSimplestVectors/1000-8            	    1000	       240.4 ns/op	      48 B/op	       1 allocs/op
BenchmarkInsertSimplestVectors/100-8             	    1000	        22.17 ns/op	       4 B/op	       0 allocs/op

$ akm@MacBook-Pro-andrey clickhouse-buffer % go test ./bench -bench=BenchmarkInsertSimplestEmptyVectors -benchmem -benchtime=1000x

BenchmarkInsertSimplestEmptyVectors/1000000-8         	    1000	    215240 ns/op	   24002 B/op	       0 allocs/op
BenchmarkInsertSimplestEmptyVectors/100000-8          	    1000	     20736 ns/op	    2400 B/op	       0 allocs/op
BenchmarkInsertSimplestEmptyVectors/10000-8           	    1000	      2109 ns/op	       0 B/op	       0 allocs/op
BenchmarkInsertSimplestEmptyVectors/1000-8            	    1000	       198.3 ns/op	      24 B/op	       0 allocs/op
BenchmarkInsertSimplestEmptyVectors/100-8             	    1000	        19.83 ns/op	       2 B/op	       0 allocs/op
```

**Conclusion:**

- buffer on redis is expected to work slower than the buffer in memory, this is due to fact that it is necessary to serialize data and deserialize it back, which causes a lot of overhead, also do not forget about network overhead
- writing through a vector causes less overhead (allocations) and works faster
- pre-allocated vector recording works very fast with zero memory allocation, this is fact that writing to buffer and then writing it to Clickhouse creates almost no overhead
- the same can be said about recording objects, but there is a small overhead
- writing vectors is faster, allocates less memory, and is preferable to writing objects
- using a buffer in-memory is preferable to a buffer in redis

### TODO:

- [ ] rewrite Buffer interface, simplify it
- [ ] rewrite Options, simplify it
- [ ] optimization redis buffer and encode/decode functions
- [ ] buffer interfaces
- [ ] more retry buffer interfaces
- [ ] rewrite retry lib, simplify it
- [ ] create binary app for streaming data to clickhouse
  - [ ] client and server with HTTP interface
  - [ ] client and server with gRPC interface
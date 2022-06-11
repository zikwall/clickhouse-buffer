[![build](https://github.com/zikwall/clickhouse-buffer/workflows/build_and_tests/badge.svg)](https://github.com/zikwall/clickhouse-buffer/v3/actions)
[![build](https://github.com/zikwall/clickhouse-buffer/workflows/golangci_lint/badge.svg)](https://github.com/zikwall/clickhouse-buffer/v3/actions)

<div align="center">
  <h1>Clickhouse Buffer</h1>
  <h5>An easy-to-use, powerful and productive package for writing data to Clickhouse columnar database</h5>
</div>

## Install

- for go-clickhouse v1 `$ go get -u github.com/zikwall/clickhouse-buffer`
- for go-clickhouse v2 `$ go get -u github.com/zikwall/clickhouse-buffer/v3`

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

    "github.com/zikwall/clickhouse-buffer/v3/src/db/cxnative"
    "github.com/zikwall/clickhouse-buffer/v3/src/db/cxsql"
)

// if you already have a connection to Clickhouse you can just use wrappers
// with native interface
ch := cxnative.NewClickhouseWithConn(driver.Conn)
// or use database/sql interface
ch := cxsql.NewClickhouseWithConn(*sql.DB)
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
})
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
}, &cxsql.RuntimeOptions{})
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
    contetx, *redis.Client, "bucket", client.Options().BatchSize(),
)
// create new writer api: table name with columns
writeAPI := client.Writer(
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

- `$ go test -v ./...`
- `$ golangci-lint run --config ./.golangci.yml`

**Integration Tests:**

```shell
export CLICKHOUSE_HOST=111.11.11.11:9000
export REDIS_HOST=111.11.11.11:6379
export REDIS_PASS=password_if_needed

$ go test -v ./... -tags=integration
```

### TODO:

- [ ] buffer interfaces
- [ ] more retry buffer interfaces
- [ ] rewrite retry lib
- [ ] create binary app for streaming data to clickhouse
  - [ ] client and server with HTTP interface
  - [ ] client and server with gRPC interface
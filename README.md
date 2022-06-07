[![build](https://github.com/zikwall/clickhouse-buffer/workflows/build_and_tests/badge.svg)](https://github.com/zikwall/clickhouse-buffer/v2/actions)
[![build](https://github.com/zikwall/clickhouse-buffer/workflows/golangci_lint/badge.svg)](https://github.com/zikwall/clickhouse-buffer/v2/actions)

<div align="center">
  <h1>Clickhouse Buffer</h1>
  <h5>An easy-to-use, powerful and productive package for writing data to Clickhouse columnar database</h5>
</div>

## Install

- for go-clickhouse v1 `$ go get -u github.com/zikwall/clickhouse-buffer`
- for go-clickhouse v2 `$ go get -u github.com/zikwall/clickhouse-buffer/v2`

## Why and why

In the practice of using the Clickhouse database (in real projects), 
you often have to resort to creating your own ~~bicycles~~ in the form of queues 
and testers that accumulate the necessary amount of data or for a certain period of time 
and send one large data package to the Clickhouse database.

This is due to the fact that Clickhouse is designed so that it better processes new data in batches 
(and this is recommended by the authors themselves).

## Features

- [x] **non-blocking** - (recommend) async write client uses implicit batching.
  Data are asynchronously written to the underlying buffer and they are automatically sent to a server
  when the size of the write buffer reaches the batch size, default 5000, or the flush interval,
  default 1s, times out. Asynchronous write client is recommended for frequent periodic writes.
- [x] **blocking.**

**Client buffer engines:**

- [x] **in-memory** - use native channels and slices
- [x] **redis** - use redis server as queue and buffer
- [x] **retries** - resending "broken" or for some reason not sent packets

## Usage

```go
import (
    "github.com/zikwall/clickhouse-buffer/v2/src/database/native"
    "github.com/zikwall/clickhouse-buffer/v2/src/database/sql"
)

// simple use
ch := native.NewClickhouseWithConn(conn: driver.Conn)
// or use database/sql
ch := sql.NewClickhouseWithConn(conn: *sql.DB)
```

```go
// another way to use
ch, conn, err := native.NewClickhouse(ctx,&clickhouse.Options{
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
        Debug: true,
    },
)
// or use database/sql
ch, conn, err := sql.NewClickhouse(ctx, &clickhouse.Options{
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
        Debug: true,
    }, &sql.RuntimeOptions{},
)
```

#### Create main data streamer client and write data

```go
import (
    chbuffer "github.com/zikwall/clickhouse-buffer"
    "github.com/zikwall/clickhouse-buffer/v2/src/buffer"
    "github.com/zikwall/clickhouse-buffer/v2/src/buffer/memory"
    "github.com/zikwall/clickhouse-buffer/v2/src/buffer/redis"
)

client := chbuffer.NewClientWithOptions(ctx, ch,
    clikchousebuffer.DefaultOptions().SetFlushInterval(1000).SetBatchSize(5000),
)

engine := memory.NewBuffer(
    client.Options().BatchSize(),
)
// or
engine := redis.NewBuffer(
    contetx, *redis.Client, "bucket", client.Options().BatchSize(),
)

writeAPI := client.Writer(buffer.View{
    Name:    "clickhouse_database.clickhouse_table", 
    Columns: []string{"id", "uuid", "insert_ts"},
}, engine)


type MyCustomDataView struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (t *MyCustomDataView) Row() chbuffer.RowSlice {
	return chbuffer.RowSlice{t.id, t.uuid, t.insertTS.Format(time.RFC822)}
}

// write your data
writeAPI.WriteRow(MyCustomDataView{
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
writerBlocking := client.WriterBlocking(View{
    Name:    "clickhouse_database.clickhouse_table",
    Columns: []string{"id", "uuid", "insert_ts"},
})

err := writerBlocking.WriteRow(ctx, []MyCustomDataView{
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

#### Retries:

> By default, packet resending is disabled, to enable it, you need to call `(*Options).SetRetryIsEnabled(true)`.

- [x] in-memory use channels (default)
- [ ] redis
- [ ] rabbitMQ
- [ ] kafka

You can implement queue engine by defining the `Queueable` interface:

```go
type Queueable interface {
	Queue(packet *retryPacket)
	Retries() <-chan *retryPacket
}
```

and set it as an engine:

```go
DefaultOptions().SetDebugMode(true).SetRetryIsEnabled(true).SetQueueEngine(CustomQueueable)
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
DefaultOptions().SetDebugMode(true).SetLogger(SomeLogger)
```

#### Tests:

- `$ go test -v ./...`
- `$ go test -v ./... -tags=integration`
- `$ golangci-lint run --config ./.golangci.yml`
[![build](https://github.com/zikwall/clickhouse-buffer/workflows/build_and_tests/badge.svg)](https://github.com/zikwall/clickhouse-buffer/actions)
[![build](https://github.com/zikwall/clickhouse-buffer/workflows/golangci_lint/badge.svg)](https://github.com/zikwall/clickhouse-buffer/actions)

# clickhouse-buffer
Buffer for streaming data to ClickHouse

## Install

- `$ go get -u github.com/zikwall/clickhouse-buffer`

## Why and why

In the practice of using the Clickhouse database (in real projects), 
you often have to resort to creating your own ~~bicycles~~ in the form of queues 
and testers that accumulate the necessary amount of data or for a certain period of time 
and send one large data package to the Clickhouse database.

This is due to the fact that Clickhouse is designed so that it better processes new data in batches 
(and this is recommended by the authors themselves).

## Features

#### Client offers two ways of writing: 

- [x] non-blocking 
- [x] blocking.

Non-blocking write client uses implicit batching. 
Data are asynchronously written to the underlying buffer and they are automatically sent to a server 
when the size of the write buffer reaches the batch size, default 5000, or the flush interval, 
default 1s, times out.

Asynchronous write client is recommended for frequent periodic writes.

#### Client buffer interfaces

- [x] in-memory
- [x] redis

#### Writes are automatically retried on server back pressure

In the future, it is necessary to implement an interface, and a mechanism for sending "rejected" packets, 
with the ability to control the number of retries, the intervals between retries. 
The "queue" data structure is a good fit for this.

- [ ] in-memory queue
- [ ] redis queue

## Usage

First you need to implement the `Rowable` interface, and your own `Row` structure for formatting the data

```go
// implement
type MyRow struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (vm MyRow) Row() types.RowSlice {
	return types.RowSlice{vm.id, vm.uuid, vm.insertTS}
}
```

Next, you need to define the Clickhouse interface, you can define your own component or use an existing implementation.

You can use two methods:
 - create a connection to the Clickhouse database from the connection parameters,

```go
clickhouse, _ := api.NewClickhouseWithOptions(&api.ClickhouseCfg{
    Address:  "my.clickhouse.host",
    Password: "",
    User:     "default",
    Database: "default",
    AltHosts: "my.clickhouse.host2,my.clickhouse.host3:9003,my.clickhouse.host4",
    IsDebug:  true,
})
```

- use an existing connection pool by providing `sqlx.DB`

```go
clickhouse, _ := api.NewClickhouseWithSqlx(*sqlx.DB)
```

#### Create main data streamer client and write data

```go
client := NewClientWithOptions(ctx, &ClickhouseImplErrMock{},
	api.DefaultOptions().SetFlushInterval(1000).SetBatchSize(5000),
)
```

You can implement your own data buffer interface: `File`, `Rabbitmq`, `CustomMemory`, etc. or use an existing one. 

```go
type Buffer interface {
	Write(vector types.RowSlice)
	Read() []types.RowSlice
	Len() int
	Flush()
}
```

Only the in-memory and redis buffer is currently available

```go
// use buffer implement interface
buffer := memory.NewBuffer(
	client.Options().BatchSize(),
)
```

```go
buffer := redis.NewBuffer(
	contetx, *redis.Client, "bucket", client.Options().BatchSize(),
)
```

Now we can write data to the necessary tables in an asynchronous, non-blocking way

```go
writeAPI := client.Writer(api.View{
    Name:    "clickhouse_database.clickhouse_table", 
    Columns: []string{"id", "uuid", "insert_ts"},
}, buffer)

// write your data
writeAPI.WriteRow(MyRow{
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
writerBlocking := client.WriterBlocking(api.View{
    Name:    "clickhouse_database.clickhouse_table",
    Columns: []string{"id", "uuid", "insert_ts"},
})

err := writerBlocking.WriteRow(ctx, []types.Rower{
    MyRow{
        id: 1, uuid: "1", insertTS: time.Now(),
    },
    MyRow{
        id: 2, uuid: "2", insertTS: time.Now(),
    },
    MyRow{
        id: 3, uuid: "3", insertTS: time.Now(),
    },
}...)
```

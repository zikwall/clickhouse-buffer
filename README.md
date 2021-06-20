# clickhouse-buffer
Buffer for streaming data to ClickHouse

## Features

#### Client offers two ways of writing: 

- [x] non-blocking 
- [ ] blocking.

Non-blocking write client uses implicit batching. 
Data are asynchronously written to the underlying buffer and they are automatically sent to a server 
when the size of the write buffer reaches the batch size, default 5000, or the flush interval, 
default 1s, times out.

Asynchronous write client is recommended for frequent periodic writes.

#### Client buffer interfaces

- [x] in-memory
- [ ] redis

#### Writes are automatically retried on server back pressure

In the future, it is necessary to implement an interface, and a mechanism for sending "rejected" packets, 
with the ability to control the number of retries, the intervals between retries. 
The "queue" data structure is a good fit for this.

- [ ] im-memory queue
- [ ] redis queue

## Usage

First you need to implement the Scalar interface, and your own Vector structure for formatting the data

```go
type Scalar interface {
    Vector() Vector
}

// implement
type MyVector struct {
	id       int
	uuid     string
	insertTs time.Time
}

func (vm MyVector) Vector() common.Vector {
	return common.Vector{vm.id, vm.uuid, vm.insertTs}
}
```

Next, you need to define the Clickhouse interface, you can define your own component or use an existing implementation.

You can use two methods:
 - create a connection to the Clickhouse database from the connection parameters,

```go
clickhouse, _ := api.NewClickhouseWithOptions(api.ClickhouseCfg{
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

You can implement your own data buffer interface or use an existing one. 

```go
type Buffer interface {
	Write(vector common.Vector)
	Read() []common.Vector
	Len() int
	Flush()
}
```

Only the in-memory buffer is currently available

```go
// use buffer implement interface
bufferImpl := buffer.NewInmemoryBuffer(
	client.Options().BatchSize(),
)
```

Now we can write data to the necessary tables in an asynchronous, non-blocking way

```go
writeAPI := client.Writer(api.View{
    Name:    "clickhouse_database.clickhouse_table", 
    Columns: []string{"id", "uuid", "insert_ts"},
}, memoryBuffer)

// write your data
writeAPI.WriteVector(MyVector{
    id: 1, uuid: "1", insertTs: time.Now(),
})
```

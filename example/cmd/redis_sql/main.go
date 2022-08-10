package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-redis/redis/v8"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v4"
	"github.com/zikwall/clickhouse-buffer/v4/example/pkg/tables"
	"github.com/zikwall/clickhouse-buffer/v4/src/buffer/cxredis"
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
	"github.com/zikwall/clickhouse-buffer/v4/src/db/cxsql"
)

func main() {
	hostname := os.Getenv("CLICKHOUSE_HOST")
	username := os.Getenv("CLICKHOUSE_USER")
	database := os.Getenv("CLICKHOUSE_DB")
	password := os.Getenv("CLICKHOUSE_PASS")
	redisHost := os.Getenv("REDIS_HOST")
	redisPass := os.Getenv("REDIS_PASS")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, conn, err := cxsql.NewClickhouse(ctx, &clickhouse.Options{
		Addr: []string{hostname},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	}, &cx.RuntimeOptions{})
	if err != nil {
		log.Panicln(err)
	}
	if err := tables.CreateTableSQL(ctx, conn); err != nil {
		log.Panicln(err)
	}
	client := clickhousebuffer.NewClientWithOptions(ctx, ch, clickhousebuffer.DefaultOptions().
		SetDebugMode(true).
		SetFlushInterval(1000).
		SetBatchSize(5),
	)
	rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPass,
	}), "bucket", client.Options().BatchSize())
	if err != nil {
		log.Panicln(err)
	}
	writeAPI := client.Writer(ctx, cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()), rxbuffer)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		errorsCh := writeAPI.Errors()
		for err := range errorsCh {
			log.Printf("clickhouse write error: %s\n", err.Error())
		}
		wg.Done()
	}()

	int32s := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, val := range int32s {
		writeAPI.WriteRow(&tables.ExampleTable{
			ID: val, UUID: fmt.Sprintf("uuidf %d", val), InsertTS: time.Now(),
		})
	}

	<-time.After(time.Second * 2)
	client.Close()
	wg.Wait()
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v4"
	"github.com/zikwall/clickhouse-buffer/v4/example/pkg/tables"
	"github.com/zikwall/clickhouse-buffer/v4/src/buffer/cxmem"
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
	"github.com/zikwall/clickhouse-buffer/v4/src/db/cxnative"
)

func main() {
	hostname := os.Getenv("CLICKHOUSE_HOST")
	username := os.Getenv("CLICKHOUSE_USER")
	database := os.Getenv("CLICKHOUSE_DB")
	password := os.Getenv("CLICKHOUSE_PASS")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, conn, err := cxnative.NewClickhouse(ctx, &clickhouse.Options{
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
	}, &cx.RuntimeOptions{
		WriteTimeout: 15 * time.Second,
	})
	if err != nil {
		log.Panicln(err)
	}
	if err := tables.CreateTableNative(ctx, conn); err != nil {
		log.Panicln(err)
	}
	client := clickhousebuffer.NewClientWithOptions(ctx, ch, clickhousebuffer.DefaultOptions().
		SetDebugMode(true).
		SetFlushInterval(1000).
		SetBatchSize(5),
	)
	writeAPI := client.Writer(
		ctx,
		cx.NewView(tables.ExampleTableName(), tables.ExampleTableColumns()),
		cxmem.NewBuffer(client.Options().BatchSize()),
	)

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

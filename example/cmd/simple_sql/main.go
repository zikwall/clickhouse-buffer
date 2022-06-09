package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	cx "github.com/zikwall/clickhouse-buffer/v2"
	"github.com/zikwall/clickhouse-buffer/v2/example/pkg/tables"
	cxmemory "github.com/zikwall/clickhouse-buffer/v2/src/buffer/memory"
	cxbase "github.com/zikwall/clickhouse-buffer/v2/src/database"
	cxsql "github.com/zikwall/clickhouse-buffer/v2/src/database/sql"
)

func main() {
	hostname := os.Getenv("CLICKHOUSE_HOST")
	username := os.Getenv("CLICKHOUSE_USER")
	database := os.Getenv("CLICKHOUSE_DB")
	password := os.Getenv("CLICKHOUSE_PASS")

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
	}, &cxsql.RuntimeOptions{})
	if err != nil {
		log.Panicln(err)
	}
	if err := tables.CreateTableSQL(ctx, conn); err != nil {
		log.Panicln(err)
	}
	client := cx.NewClientWithOptions(ctx, ch,
		cx.DefaultOptions().SetDebugMode(true).SetFlushInterval(1000).SetBatchSize(5),
	)

	writeAPI := client.Writer(cxbase.View{
		Name:    tables.ExampleTableName(),
		Columns: tables.ExampleTableColumns(),
	}, cxmemory.NewBuffer(client.Options().BatchSize()))

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
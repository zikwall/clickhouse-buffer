package main

import (
	"context"
	"encoding/gob"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v3"
	"github.com/zikwall/clickhouse-buffer/v3/example/pkg/tables"
	"github.com/zikwall/clickhouse-buffer/v3/src/buffer/cxredis"
	"github.com/zikwall/clickhouse-buffer/v3/src/cx"
	"github.com/zikwall/clickhouse-buffer/v3/src/db/cxnative"
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

	if err := tables.CreateAdvancedTableNative(ctx, conn); err != nil {
		log.Panicln(err)
	}

	client := clickhousebuffer.NewClientWithOptions(ctx, ch, clickhousebuffer.DefaultOptions().
		SetDebugMode(true).
		SetFlushInterval(1000).
		SetBatchSize(10),
	)
	rxbuffer, err := cxredis.NewBuffer(ctx, redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: redisPass,
		DB:       10,
	}), "bucket", client.Options().BatchSize())
	if err != nil {
		log.Panicln(err)
	}
	writeAPI := client.Writer(
		ctx,
		cx.NewView(tables.AdvancedTableName(), tables.AdvancedTableColumns()),
		rxbuffer,
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

	// before register data types
	gob.Register(uuid.UUID{})
	// Tuple
	gob.Register([]interface{}{})
	gob.Register(time.Time{})
	// array of Tuple
	gob.Register([][]interface{}{})

	write(writeAPI)

	<-time.After(time.Second * 2)
	client.Close()
	wg.Wait()
}

// nolint:gocritic // it's OK
func write(writeAPI clickhousebuffer.Writer) {
	for i := 0; i < 50; i++ {
		writeAPI.WriteRow(&tables.AdvancedTable{
			Col1: uint8(42),
			Col2: "ClickHouse",
			Col3: "Inc",
			Col4: uuid.New(),
			// Map(String, UInt8)
			// Col5: map[string]uint8{"key": 1},
			// Array(String)
			Col6: []string{"Q", "W", "E", "R", "T", "Y"},
			// Tuple(String, UInt8, Array(Map(String, String)))
			Col7: []interface{}{
				"String Value", uint8(5), []string{"val1", "val2", "val3"}, []interface{}{
					time.Now(),
					uint32(5),
				},
			},
			Col8:  time.Now(),
			Col9:  "hello",
			Col10: time.Now(),
			Col11: b2i8(i%2 == 0),
			Col12: time.Now(),
			Col13: [][]interface{}{
				{
					"String Value", uint8(5), []string{"val1", "val2", "val3"}, []interface{}{
						time.Now(),
						uint32(5),
					},
				},
				{
					"String Value", uint8(5), []string{"val1", "val2", "val3"}, []interface{}{
						time.Now(),
						uint32(5),
					},
				},
			},
		})
	}
}

// clickhouse doesn't support bool
func b2i8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}

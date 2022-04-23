//go:build integration
// +build integration

package clickhousebuffer

import (
	"context"
	"log"
	"testing"

	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"github.com/zikwall/clickhouse-buffer/src/buffer/memory"
)

func TestMemory(t *testing.T) {
	var err error
	log.Println("RUN INTEGRATION TEST WITH MEMORY AND CLICKHOUSE")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STEP 2: Create Clickhouse service
	pool, resource, ch, clickhouse, err := useClickhousePool(ctx)
	if err != nil {
		log.Panic(err)
	}

	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTables(ctx, ch); err != nil {
		log.Panic(err)
	}

	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, memBuffer := useClientAndMemoryBuffer(ctx, clickhouse)
	defer client.Close()

	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(client, memBuffer)
	writeDataToBuffer(writeAPI)

	// STEP 6: Checks!
	if err = checksBuffer(memBuffer); err != nil {
		log.Panic(err)
	}

	if err = checksClickhouse(ctx, ch); err != nil {
		log.Panic(err)
	}

	// STEP 7: Close resources
	// You can't defer this because os.Exit doesn't care for defer
	if err = pool.Purge(resource); err != nil {
		log.Panicf("could't purge resource: %s", err)
	}
}

func useClientAndMemoryBuffer(ctx context.Context, clickhouse Clickhouse) (Client, buffer.Buffer) {
	client := useCommonClient(ctx, clickhouse)
	return client, memory.NewBuffer(client.Options().BatchSize())
}

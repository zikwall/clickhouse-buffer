// +build integration

package clickhousebuffer

import (
	"context"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"github.com/zikwall/clickhouse-buffer/src/buffer/memory"
	"log"
	"testing"
)

func TestMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STEP 2: Create Clickhouse service
	pool, resource, ch, clickhouse, err := useClickhousePool()

	if err != nil {
		log.Fatal(err)
	}

	// STEP 3: Drop and Create table under certain conditions
	if beforeCheckTables(ctx, ch); err != nil {
		log.Fatal(err)
	}

	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, memBuffer := useClientAndMemoryBuffer(ctx, clickhouse)

	// STEP 5: Write own data to redis
	writeDataToBuffer(client, memBuffer)

	// STEP 6: Checks!
	if err := checksBuffer(memBuffer); err != nil {
		log.Fatal(err)
	}

	if err := checksClickhouse(ctx, ch); err != nil {
		log.Fatal(err)
	}

	// STEP 7: Close resources
	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
}

func useClientAndMemoryBuffer(ctx context.Context, clickhouse Clickhouse) (Client, buffer.Buffer) {
	client := NewClientWithOptions(ctx, clickhouse,
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)

	return client, memory.NewBuffer(client.Options().BatchSize())
}

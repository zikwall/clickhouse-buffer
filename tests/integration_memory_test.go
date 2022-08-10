//go:build integration
// +build integration

package tests

import (
	"context"
	"log"
	"testing"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v4"
	"github.com/zikwall/clickhouse-buffer/v4/src/buffer/cxsyncmem"
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

// nolint:dupl // it's OK
func TestMemory(t *testing.T) {
	var err error
	log.Println("RUN INTEGRATION TEST WITH MEMORY AND NATIVE CLICKHOUSE")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// STEP 2: Create Clickhouse service
	ch, clickhouse, err := useClickhousePool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTables(ctx, ch); err != nil {
		t.Fatal(err)
	}
	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, memBuffer := useClientAndMemoryBuffer(ctx, clickhouse)
	defer client.Close()
	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(ctx, client, memBuffer)
	writeDataToBuffer(writeAPI)
	// STEP 6: Checks!
	if err = checksBuffer(memBuffer); err != nil {
		t.Fatal(err)
	}
	if err = checksClickhouse(ctx, ch); err != nil {
		t.Fatal(err)
	}
}

// nolint:dupl // it's OK
func TestSQLMemory(t *testing.T) {
	var err error
	log.Println("RUN INTEGRATION TEST WITH MEMORY AND SQL CLICKHOUSE")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// STEP 2: Create Clickhouse service
	ch, clickhouse, err := useClickhouseSQLPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTablesSQL(ctx, ch); err != nil {
		t.Fatal(err)
	}
	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, memBuffer := useClientAndMemoryBuffer(ctx, clickhouse)
	defer client.Close()
	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(ctx, client, memBuffer)
	writeDataToBuffer(writeAPI)
	// STEP 6: Checks!
	if err = checksBuffer(memBuffer); err != nil {
		t.Fatal(err)
	}
	if err = checksClickhouseSQL(ctx, ch); err != nil {
		t.Fatal(err)
	}
}

// nolint:dupl // it's OK
func TestMemorySafe(t *testing.T) {
	var err error
	log.Println("RUN INTEGRATION TEST WITH MEMORY AND NATIVE CLICKHOUSE [SAFE]")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// STEP 2: Create Clickhouse service
	ch, clickhouse, err := useClickhousePool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTables(ctx, ch); err != nil {
		t.Fatal(err)
	}
	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, memBuffer := useClientAndMemoryBuffer(ctx, clickhouse)
	defer client.Close()
	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(ctx, client, memBuffer)
	writeDataToBufferSafe(writeAPI)
	// STEP 6: Checks!
	if err = checksBuffer(memBuffer); err != nil {
		t.Fatal(err)
	}
	if err = checksClickhouse(ctx, ch); err != nil {
		t.Fatal(err)
	}
}

func useClientAndMemoryBuffer(ctx context.Context, clickhouse cx.Clickhouse) (clickhousebuffer.Client, cx.Buffer) {
	client := useCommonClient(ctx, clickhouse)
	return client, cxsyncmem.NewBuffer(client.Options().BatchSize())
}

// +build integration

package clickhousebuffer

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/ory/dockertest/v3"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	redis2 "github.com/zikwall/clickhouse-buffer/src/buffer/redis"
	"log"
	"os"
	"testing"
	"time"
)

type ClickhouseImplIntegration struct{}

func (ch *ClickhouseImplIntegration) Insert(_ context.Context, _ View, _ []buffer.RowSlice) (uint64, error) {
	return 0, nil
}

type IntegrationRow struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (i IntegrationRow) Row() buffer.RowSlice {
	return buffer.RowSlice{i.id, i.uuid, i.insertTS.Format(time.RFC822)}
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClientWithOptions(ctx, &ClickhouseImplIntegration{},
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)

	var db *redis.Client

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("redis", "6.2", nil)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		db = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})

		return db.Ping(db.Context()).Err()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	redisBuffer, err := redis2.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())
	if err != nil {
		log.Fatalf("Could not create redis buffer: %s", err)
	}

	writeAPI := client.Writer(View{
		Name:    "clickhouse_database.clickhouse_table",
		Columns: []string{"id", "uuid", "insert_ts"},
	}, redisBuffer)

	writeAPI.WriteRow(IntegrationRow{
		id: 1, uuid: "1", insertTS: time.Now(),
	})
	writeAPI.WriteRow(IntegrationRow{
		id: 2, uuid: "2", insertTS: time.Now(),
	})
	writeAPI.WriteRow(IntegrationRow{
		id: 3, uuid: "3", insertTS: time.Now(),
	})
	writeAPI.WriteRow(IntegrationRow{
		id: 4, uuid: "4", insertTS: time.Now(),
	})
	writeAPI.WriteRow(IntegrationRow{
		id: 5, uuid: "5", insertTS: time.Now(),
	})

	rows := redisBuffer.Read()
	if len(rows) != 5 {
		log.Fatalf("Could not get correct valuse, received: %v", rows)
	}
	log.Printf("Received value: %v", rows)

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestSomething(t *testing.T) {
	// db.Query()
}

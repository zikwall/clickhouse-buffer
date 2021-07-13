// +build integration

package clickhousebuffer

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
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

	var db *redis.Client
	var ch *sqlx.DB
	var clickhouse Clickhouse

	// REDIS
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

	// CLICKHOUSE
	pool2, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource2, err := pool2.Run("yandex/clickhouse-server", "20.8.19.4", nil)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool2.Retry(func() error {
		ch, err := sqlx.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")

		if err != nil {
			return err
		}

		clickhouse, err = NewClickhouseWithSqlx(ch)

		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	_, err = ch.Exec(`
		CREATE TABLE IF NOT EXISTS default.test_integration (
			id        	UInt8,
			uuid   		String,
			insert_ts   String,
		) engine=Memory
	`)

	if err != nil {
		log.Fatalf("Could create clickhouse table: %s", err)
	}

	client := NewClientWithOptions(ctx, clickhouse,
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)

	redisBuffer, err := redis2.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())
	if err != nil {
		log.Fatalf("Could not create redis buffer: %s", err)
	}

	writeAPI := client.Writer(View{
		Name:    "default.test_integration",
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

	<-time.After(500 * time.Millisecond)

	if size := redisBuffer.Len(); size != 0 {
		log.Fatal("Failed, the buffer was expected to be cleared")
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	if err := pool2.Purge(resource2); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestSomething(t *testing.T) {
	// db.Query()
}

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

const IntegrationTableName = "default.test_integration_xxx_xxx"

type IntegrationRow struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (i IntegrationRow) Row() buffer.RowSlice {
	return buffer.RowSlice{i.id, i.uuid, i.insertTS.Format(time.RFC822)}
}

// This test is a complete simulation of the work of the buffer bundle (Redis) and the Clickhouse data warehouse
func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var db *redis.Client
	var ch *sqlx.DB
	var clickhouse Clickhouse

	// STEP 1: Create Redis serrvice
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to redis docker: %s", err)
	}

	resource, err := pool.Run("redis", "6.2", nil)
	if err != nil {
		log.Fatalf("Could not start redis resource: %s", err)
	}

	if err := pool.Retry(func() error {
		db = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})

		return db.Ping(db.Context()).Err()
	}); err != nil {
		log.Fatalf("Could not connect to redis docker: %s", err)
	}

	// STEP 2: Create Clickhouse service
	pool2, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to clickhouse docker: %s", err)
	}

	resource2, err := pool2.Run("yandex/clickhouse-server", "20.8.19.4", nil)
	if err != nil {
		log.Fatalf("Could not start clickhouse resource: %s", err)
	}

	if err := pool2.Retry(func() error {
		ch, err = sqlx.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
		if err != nil {
			return err
		}

		// auto Ping by clickhouse buffer package
		clickhouse, err = NewClickhouseWithSqlx(ch)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to clickhouse docker: %s", err)
	}

	// STEP 3: Drop and Create table under certain conditions
	_, _ = ch.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", IntegrationTableName))
	_, err = ch.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id        	UInt8,
			uuid   		String,
			insert_ts   String
		) engine=Memory
	`, IntegrationTableName))

	if err != nil {
		log.Fatalf("Could create clickhouse table: %s", err)
	}

	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client := NewClientWithOptions(ctx, clickhouse,
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)

	redisBuffer, err := redis2.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())
	if err != nil {
		log.Fatalf("Could not create redis buffer: %s", err)
	}

	writeAPI := client.Writer(View{
		Name:    IntegrationTableName,
		Columns: []string{"id", "uuid", "insert_ts"},
	}, redisBuffer)

	// STEP 5: Write own data to redis
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

	// STEP 6: Tests

	// wait a bit
	<-time.After(50 * time.Millisecond)

	// try read from redis buffer before flushing data in storage (Redis)
	rows := redisBuffer.Read()
	if len(rows) != 5 {
		log.Fatalf("Could not get correct valuse, received: %v", rows)
	}
	log.Printf("Received value: %v", rows)

	// wait until flush in redis buffer
	<-time.After(500 * time.Millisecond)

	// check redis buffer size
	if size := redisBuffer.Len(); size != 0 {
		log.Fatal("Failed, the buffer was expected to be cleared")
	}

	// check data in clickhouse, write after flushing
	values, err := fetchClickhouseRows(ctx, ch)
	if err != nil {
		log.Fatalf("Could not fetch data from clickhouse: %s", err)
	}

	log.Printf("Received values from clickhouse table: %v", values)

	if len(values) != 5 {
		log.Fatalf("Failed, expected to get five values, received %d", len(values))
	}

	if v := values[2][1]; v != "3" {
		log.Fatalf("Failed, expected value 3, received %s", v)
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

func fetchClickhouseRows(ctx context.Context, ch *sqlx.DB) ([][]interface{}, error) {
	rws, err := ch.QueryContext(ctx, fmt.Sprintf("SELECT id, uuid, insert_ts FROM %s", IntegrationTableName))
	if err != nil {
		return nil, err
	}
	defer rws.Close()

	var values [][]interface{}
	for rws.Next() {
		var (
			id        uint8
			uuid      string
			createdAt string
		)

		if err := rws.Scan(&id, &uuid, &createdAt); err != nil {
			return nil, err
		}

		values = append(values, []interface{}{id, uuid, createdAt})
	}

	if err := rws.Err(); err != nil {
		return nil, err
	}

	return values, err
}

func TestSomething(t *testing.T) {
	// db.Query()
}

// +build integration

package clickhousebuffer

import (
	"context"
	"errors"
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

const integrationTableName = "default.test_integration_xxx_xxx"

type integrationRow struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (i integrationRow) Row() buffer.RowSlice {
	return buffer.RowSlice{i.id, i.uuid, i.insertTS.Format(time.RFC822)}
}

// This test is a complete simulation of the work of the buffer bundle (Redis) and the Clickhouse data warehouse
func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var db *redis.Client
	var ch *sqlx.DB
	var clickhouse Clickhouse

	// STEP 1: Create Redis service
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
	if beforeCheckTables(ctx, ch); err != nil {
		log.Fatal(err)
	}

	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, redisBuffer, err := useClientAndRedisBuffer(ctx, clickhouse, db)

	if err != nil {
		log.Fatal(err)
	}

	// STEP 5: Write own data to redis
	writeDataToBuffer(client, redisBuffer)

	// STEP 6: Checks!
	if err := checksBuffer(redisBuffer); err != nil {
		log.Fatal(err)
	}

	if err := checksClickhouse(ctx, ch); err != nil {
		log.Fatal(err)
	}

	// STEP 7: Close resources
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

type clickhouseRowData struct {
	id        uint8
	uuid      string
	createdAt string
}

func fetchClickhouseRows(ctx context.Context, ch *sqlx.DB) ([]clickhouseRowData, error) {
	rws, err := ch.QueryContext(ctx, fmt.Sprintf("SELECT id, uuid, insert_ts FROM %s", integrationTableName))
	if err != nil {
		return nil, err
	}
	defer rws.Close()

	var values []clickhouseRowData
	for rws.Next() {
		var (
			id        uint8
			uuid      string
			createdAt string
		)

		if err := rws.Scan(&id, &uuid, &createdAt); err != nil {
			return nil, err
		}

		values = append(values, clickhouseRowData{id, uuid, createdAt})
	}

	if err := rws.Err(); err != nil {
		return nil, err
	}

	return values, err
}

func beforeCheckTables(ctx context.Context, ch *sqlx.DB) error {
	_, _ = ch.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTableName))
	_, err := ch.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id        	UInt8,
			uuid   		String,
			insert_ts   String
		) engine=Memory
	`, integrationTableName))

	return err
}

func useClientAndRedisBuffer(ctx context.Context, clickhouse Clickhouse, db *redis.Client) (Client, buffer.Buffer, error) {
	client := NewClientWithOptions(ctx, clickhouse,
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)

	buf, err := redis2.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())

	if err != nil {
		return nil, nil, fmt.Errorf("Could not create redis buffer: %s", err)
	}

	return client, buf, nil
}

func writeDataToBuffer(client Client, buf buffer.Buffer) {
	writeAPI := client.Writer(View{
		Name:    integrationTableName,
		Columns: []string{"id", "uuid", "insert_ts"},
	}, buf)

	writeAPI.WriteRow(integrationRow{
		id: 1, uuid: "1", insertTS: time.Now(),
	})
	writeAPI.WriteRow(integrationRow{
		id: 2, uuid: "2", insertTS: time.Now(),
	})
	writeAPI.WriteRow(integrationRow{
		id: 3, uuid: "3", insertTS: time.Now(),
	})
	writeAPI.WriteRow(integrationRow{
		id: 4, uuid: "4", insertTS: time.Now(),
	})
	writeAPI.WriteRow(integrationRow{
		id: 5, uuid: "5", insertTS: time.Now(),
	})

	// wait a bit
	<-time.After(50 * time.Millisecond)
}

func checksBuffer(buf buffer.Buffer) error {
	// try read from redis buffer before flushing data in storage (Redis)
	rows := buf.Read()

	if len(rows) != 5 {
		return fmt.Errorf("Could not get correct valuse, received: %v", rows)
	}

	log.Printf("Received value from buffer: %v", rows)

	// wait until flush in redis buffer
	<-time.After(500 * time.Millisecond)

	// check redis buffer size
	if size := buf.Len(); size != 0 {
		errors.New("Failed, the buffer was expected to be cleared")
	}

	return nil
}

func checksClickhouse(ctx context.Context, ch *sqlx.DB) error {
	// check data in clickhouse, write after flushing
	values, err := fetchClickhouseRows(ctx, ch)
	if err != nil {
		fmt.Errorf("Could not fetch data from clickhouse: %s", err)
	}

	log.Printf("Received values from clickhouse table: %v", values)

	if len(values) != 5 {
		fmt.Errorf("Failed, expected to get five values, received %d", len(values))
	}

	if v := values[2].id; v != 3 {
		fmt.Errorf("Failed, expected value 3, received %d", v)
	}

	if v := values[2].uuid; v != "3" {
		fmt.Errorf("Failed, expected value 3, received %s", v)
	}

	return nil
}

func TestSomething(t *testing.T) {
	// db.Query()
}

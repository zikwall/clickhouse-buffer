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
	log.Println("RUN INTEGRATION TEST WITH REDIS AND CLICKHOUSE")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STEP 1: Create Redis service
	pool, resource, db, err := useRedisPool()

	if err != nil {
		log.Fatal(err)
	}

	// STEP 2: Create Clickhouse service
	pool2, resource2, ch, clickhouse, err := useClickhousePool()

	if err != nil {
		log.Fatal(err)
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

	defer client.Close()

	// STEP 5: Write own data to redis
	writeAPI := useWriteApi(client, redisBuffer)

	var errors []error
	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			errors = append(errors, err)
		}
	}()

	writeDataToBuffer(writeAPI)

	// STEP 6: Checks!
	if err := checksBuffer(redisBuffer); err != nil {
		log.Fatal(err)
	}

	if err := checksClickhouse(ctx, ch); err != nil {
		log.Fatal(err)
	}

	// retry test fails
	dropTable(ctx, ch)
	// downgrade timeout for insert, this is necessary because we are waiting for a very long time (15s default)
	// but since we deleted the table, we don't need to wait that long
	if impl, ok := clickhouse.(*ClickhouseImpl); ok {
		impl.SetInsertTimeout(500)
	}

	// it should be successful case
	writeDataToBuffer(writeAPI)
	if err := checksBuffer(redisBuffer); err != nil {
		log.Fatal(err)
	}

	// we expect an exception from Clickhouse: code: 60, message: Table default.test_integration_xxx_xxx doesn't exist
	<-time.After(600 * time.Millisecond)

	if len(errors) != 1 {
		log.Fatalf("Failed, the clickhouse was expected receive one error, received: %d", len(errors))
	}

	log.Println("Received errors from clickhouse insert:", errors)

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
	dropTable(ctx, ch)
	return createTable(ctx, ch)
}

func dropTable(ctx context.Context, ch *sqlx.DB) {
	_, _ = ch.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTableName))
}

func createTable(ctx context.Context, ch *sqlx.DB) error {
	_, err := ch.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id        	UInt8,
			uuid   		String,
			insert_ts   String
		) engine=Memory
	`, integrationTableName))
	return err
}

func useRedisPool() (*dockertest.Pool, *dockertest.Resource, *redis.Client, error) {
	pool, err := dockertest.NewPool("")

	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not connect to redis docker: %s", err)
	}

	resource, err := pool.Run("redis", "6.2", nil)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not start redis resource: %s", err)
	}

	var db *redis.Client

	err = pool.Retry(func() error {
		db = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})

		return db.Ping(db.Context()).Err()
	})

	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not connect to redis docker: %s", err)
	}

	return pool, resource, db, nil
}

func useClickhousePool() (*dockertest.Pool, *dockertest.Resource, *sqlx.DB, Clickhouse, error) {
	pool, err := dockertest.NewPool("")

	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Could not connect to clickhouse docker: %s", err)
	}

	resource, err := pool.Run("yandex/clickhouse-server", "20.8.19.4", nil)

	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Could not start clickhouse resource: %s", err)
	}

	var ch *sqlx.DB
	var clickhouse Clickhouse

	err = pool.Retry(func() error {
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
	})

	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Could not connect to clickhouse docker: %s", err)
	}

	return pool, resource, ch, clickhouse, nil
}

func useCommonClient(ctx context.Context, clickhouse Clickhouse) Client {
	return NewClientWithOptions(ctx, clickhouse,
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)
}

func useClientAndRedisBuffer(ctx context.Context, clickhouse Clickhouse, db *redis.Client) (Client, buffer.Buffer, error) {
	client := useCommonClient(ctx, clickhouse)
	buf, err := redis2.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())

	if err != nil {
		return nil, nil, fmt.Errorf("Could not create redis buffer: %s", err)
	}

	return client, buf, nil
}

func useWriteApi(client Client, buf buffer.Buffer) Writer {
	writeAPI := client.Writer(View{
		Name:    integrationTableName,
		Columns: []string{"id", "uuid", "insert_ts"},
	}, buf)
	return writeAPI
}

func writeDataToBuffer(writeAPI Writer) {
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
	// try read from redis buffer before flushing data in buffer
	rows := buf.Read()

	if len(rows) != 5 {
		return fmt.Errorf("Could not get correct valuse, received: %v", rows)
	}

	log.Printf("Received value from buffer: %v", rows)

	// wait until flush in  buffer
	<-time.After(500 * time.Millisecond)

	// check buffer size
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

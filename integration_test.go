//go:build integration
// +build integration

package clickhousebuffer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/zikwall/clickhouse-buffer/src/buffer"
	redis2 "github.com/zikwall/clickhouse-buffer/src/buffer/redis"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-redis/redis/v8"
	"github.com/ory/dockertest/v3"
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
	var err error
	log.Println("RUN INTEGRATION TEST WITH REDIS AND CLICKHOUSE")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STEP 1: Create Redis service
	pool, resource, db, err := useRedisPool()
	if err != nil {
		log.Panic(err)
	}

	// STEP 2: Create Clickhouse service
	pool2, resource2, conn, nativeClickhouse, err := useClickhousePool(ctx)
	if err != nil {
		log.Panic(err)
	}

	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTables(ctx, conn); err != nil {
		log.Panic(err)
	}

	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, redisBuffer, err := useClientAndRedisBuffer(ctx, nativeClickhouse, db)
	if err != nil {
		log.Panic(err)
	}

	defer client.Close()

	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(client, redisBuffer)

	var errorsSlice []error
	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			errorsSlice = append(errorsSlice, err)
		}
	}()

	writeDataToBuffer(writeAPI)

	// STEP 6: Checks!
	if err := checksBuffer(redisBuffer); err != nil {
		log.Panic(err)
	}

	if err := checksClickhouse(ctx, conn); err != nil {
		log.Panic(err)
	}

	// retry test fails
	dropTable(ctx, conn)

	// it should be successful case
	writeDataToBuffer(writeAPI)
	if err := checksBuffer(redisBuffer); err != nil {
		log.Panic(err)
	}

	// we expect an exception from Clickhouse: code: 60, message: Table default.test_integration_xxx_xxx doesn't exist
	<-time.After(600 * time.Millisecond)

	if len(errorsSlice) != 1 {
		log.Panicf("failed, the clickhouse was expected receive one error, received: %d", len(errorsSlice))
	}

	log.Println("received errors from clickhouse insert:", errorsSlice)

	// STEP 7: Close resources
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Panicf("could not purge resource: %s", err)
	}

	if err := pool2.Purge(resource2); err != nil {
		log.Panicf("could not purge resource: %s", err)
	}
	// nolint:gocritic // it's OK
	os.Exit(code)
}

type clickhouseRowData struct {
	id        uint8
	uuid      string
	createdAt string
}

func fetchClickhouseRows(ctx context.Context, conn driver.Conn) ([]clickhouseRowData, error) {
	rws, err := conn.Query(ctx, fmt.Sprintf("SELECT id, uuid, insert_ts FROM %s", integrationTableName))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rws.Close()
	}()
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

func beforeCheckTables(ctx context.Context, conn driver.Conn) error {
	dropTable(ctx, conn)
	return createTable(ctx, conn)
}

func dropTable(ctx context.Context, conn driver.Conn) {
	_ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTableName))
}

func createTable(ctx context.Context, conn driver.Conn) error {
	err := conn.Exec(ctx, fmt.Sprintf(`
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
		return nil, nil, nil, fmt.Errorf("coul' not connect to redis docker: %s", err)
	}

	resource, err := pool.Run("redis", "6.2", nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could 't start redis resource: %s", err)
	}

	var db *redis.Client
	err = pool.Retry(func() error {
		db = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})
		return db.Ping(db.Context()).Err()
	})

	if err != nil {
		return nil, nil, nil, fmt.Errorf("could't connect to redis docker: %s", err)
	}
	return pool, resource, db, nil
}

func useClickhousePool(ctx context.Context) (*dockertest.Pool, *dockertest.Resource, driver.Conn, Clickhouse, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("could't connect to clickhouse docker: %s", err)
	}

	resource, err := pool.Run("yandex/clickhouse-server", "20.8.19.4", nil)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("could't start clickhouse resource: %s", err)
	}

	var (
		conn             driver.Conn
		nativeClickhouse Clickhouse
	)

	err = pool.Retry(func() error {
		// auto Ping by clickhouse buffer package
		nativeClickhouse, err = NewNativeClickhouse(ctx, &clickhouse.Options{
			Addr: []string{"localhost"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout:     5 * time.Second,
			MaxIdleConns:    5,
			MaxOpenConns:    5,
			ConnMaxLifetime: time.Minute * 5,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Debug: true,
		})
		if err != nil {
			return err
		}
		conn = nativeClickhouse.Conn()
		return nil
	})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("could't connect to clickhouse docker: %s", err)
	}
	return pool, resource, conn, nativeClickhouse, nil
}

func useCommonClient(ctx context.Context, clickhouse Clickhouse) Client {
	return NewClientWithOptions(ctx, clickhouse,
		DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)
}

func useClientAndRedisBuffer(
	ctx context.Context,
	clickhouse Clickhouse,
	db *redis.Client,
) (
	Client,
	buffer.Buffer,
	error,
) {
	client := useCommonClient(ctx, clickhouse)
	buf, err := redis2.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())
	if err != nil {
		return nil, nil, fmt.Errorf("could't create redis buffer: %s", err)
	}
	return client, buf, nil
}

func useWriteAPI(client Client, buf buffer.Buffer) Writer {
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
		return fmt.Errorf("could't get correct valuse, received: %v", rows)
	}
	log.Printf("Received value from buffer: %v", rows)
	// wait until flush in  buffer
	<-time.After(500 * time.Millisecond)
	// check buffer size
	if size := buf.Len(); size != 0 {
		return errors.New("failed, the buffer was expected to be cleared")
	}
	return nil
}

func checksClickhouse(ctx context.Context, conn driver.Conn) error {
	// check data in clickhouse, write after flushing
	values, err := fetchClickhouseRows(ctx, conn)
	if err != nil {
		return fmt.Errorf("could't fetch data from clickhouse: %s", err)
	}
	log.Printf("Received values from clickhouse table: %v", values)
	if len(values) != 5 {
		return fmt.Errorf("failed, expected to get five values, received %d", len(values))
	}
	if v := values[2].id; v != 3 {
		return fmt.Errorf("failed, expected value 3, received %d", v)
	}
	if v := values[2].uuid; v != "3" {
		return fmt.Errorf("failed, expected value 3, received %s", v)
	}
	return nil
}

func TestSomething(t *testing.T) {
	// db.Query()
}

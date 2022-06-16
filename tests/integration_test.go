//go:build integration
// +build integration

package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-redis/redis/v8"

	clickhousebuffer "github.com/zikwall/clickhouse-buffer/v3"
	"github.com/zikwall/clickhouse-buffer/v3/src/buffer/cxredis"
	"github.com/zikwall/clickhouse-buffer/v3/src/cx"
	"github.com/zikwall/clickhouse-buffer/v3/src/db/cxnative"
	"github.com/zikwall/clickhouse-buffer/v3/src/db/cxsql"
)

const integrationTableName = "default.test_integration_xxx_xxx"

type integrationRow struct {
	id       uint8
	uuid     string
	insertTS time.Time
}

func (i integrationRow) Row() cx.Vector {
	return cx.Vector{i.id, i.uuid, i.insertTS.Format(time.RFC822)}
}

// This test is a complete simulation of the work of the buffer bundle (Redis) and the Clickhouse data warehouse
// nolint:dupl // it's OK
func TestNative(t *testing.T) {
	var err error
	log.Println("RUN INTEGRATION TEST WITH REDIS AND NATIVE CLICKHOUSE")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// STEP 1: Create Redis service
	db, err := useRedisPool()
	if err != nil {
		t.Fatal(err)
	}
	// STEP 2: Create Clickhouse service
	conn, nativeClickhouse, err := useClickhousePool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTables(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, redisBuffer, err := useClientAndRedisBuffer(ctx, nativeClickhouse, db)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(client, redisBuffer)
	var errorsSlice []error
	mu := &sync.RWMutex{}
	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			mu.Lock()
			errorsSlice = append(errorsSlice, err)
			mu.Unlock()
		}
	}()
	writeDataToBuffer(writeAPI)
	// STEP 6: Checks!
	if err := checksBuffer(redisBuffer); err != nil {
		t.Fatal(err)
	}
	if err := checksClickhouse(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// retry test fails
	dropTable(ctx, conn)
	// it should be successful case
	writeDataToBuffer(writeAPI)
	if err := checksBuffer(redisBuffer); err != nil {
		t.Fatal(err)
	}
	// we expect an exception from Clickhouse: code: 60, message: Table default.test_integration_xxx_xxx doesn't exist
	<-time.After(1000 * time.Millisecond)
	mu.RLock()
	defer mu.RUnlock()
	if len(errorsSlice) != 1 {
		t.Fatalf("failed, the clickhouse was expected receive one error, received: %d", len(errorsSlice))
	}
	log.Println("received errors from clickhouse insert:", errorsSlice)
}

// nolint:dupl // it's OK
func TestSQL(t *testing.T) {
	var err error
	log.Println("RUN INTEGRATION TEST WITH REDIS AND SQL CLICKHOUSE")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// STEP 1: Create Redis service
	db, err := useRedisPool()
	if err != nil {
		t.Fatal(err)
	}
	// STEP 2: Create Clickhouse service
	conn, nativeClickhouse, err := useClickhouseSQLPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// STEP 3: Drop and Create table under certain conditions
	if err = beforeCheckTablesSQL(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// STEP 4: Create clickhouse client and buffer writer with redis buffer
	client, redisBuffer, err := useClientAndRedisBuffer(ctx, nativeClickhouse, db)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// STEP 5: Write own data to redis
	writeAPI := useWriteAPI(client, redisBuffer)
	var errorsSlice []error
	mu := &sync.RWMutex{}
	errorsCh := writeAPI.Errors()
	go func() {
		for err := range errorsCh {
			mu.Lock()
			errorsSlice = append(errorsSlice, err)
			mu.Unlock()
		}
	}()
	writeDataToBuffer(writeAPI)
	// STEP 6: Checks!
	if err := checksBuffer(redisBuffer); err != nil {
		t.Fatal(err)
	}
	if err := checksClickhouseSQL(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// retry test fails
	dropTableSQL(ctx, conn)
	// it should be successful case
	writeDataToBuffer(writeAPI)
	if err := checksBuffer(redisBuffer); err != nil {
		t.Fatal(err)
	}
	// we expect an exception from Clickhouse: code: 60, message: Table default.test_integration_xxx_xxx doesn't exist
	<-time.After(1000 * time.Millisecond)
	mu.RLock()
	defer mu.RUnlock()
	if len(errorsSlice) != 1 {
		t.Fatalf("failed, the clickhouse was expected receive one error, received: %d", len(errorsSlice))
	}
	log.Println("received errors from clickhouse insert:", errorsSlice)
}

type clickhouseRowData struct {
	id        uint8
	uuid      string
	createdAt string
}

// nolint:dupl // it's OK
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

// nolint:dupl // it's OK
func fetchClickhouseRowsSQL(ctx context.Context, conn *sql.DB) ([]clickhouseRowData, error) {
	rws, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT id, uuid, insert_ts FROM %s", integrationTableName))
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

// nolint:dupl // it's OK
func beforeCheckTables(ctx context.Context, conn driver.Conn) error {
	dropTable(ctx, conn)
	return createTable(ctx, conn)
}

// nolint:dupl // it's OK
func dropTable(ctx context.Context, conn driver.Conn) {
	_ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTableName))
}

// nolint:dupl // it's OK
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

// nolint:dupl // it's OK
func beforeCheckTablesSQL(ctx context.Context, conn *sql.DB) error {
	dropTableSQL(ctx, conn)
	return createTableSQL(ctx, conn)
}

// nolint:dupl // it's OK
func dropTableSQL(ctx context.Context, conn *sql.DB) {
	_, _ = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTableName))
}

// nolint:dupl // it's OK
func createTableSQL(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id        	UInt8,
			uuid   		String,
			insert_ts   String
		) engine=Memory
	`, integrationTableName))
	return err
}

func useRedisPool() (*redis.Client, error) {
	var (
		db   *redis.Client
		host = os.Getenv("REDIS_HOST")
		user = os.Getenv("REDIS_USER")
		pass = os.Getenv("REDIS_PASS")
	)
	if host == "" {
		host = "localhost:6379"
	}
	db = redis.NewClient(&redis.Options{
		Addr:     host,
		Username: user,
		Password: pass,
		DB:       12,
	})
	if err := db.Ping(db.Context()).Err(); err != nil {
		return nil, err
	}
	return db, nil
}

const (
	defaultUser = "default"
	defaultDb   = "default"
	defaultHost = "localhost:9000"
)

func useCredentials() (host, db, user, password string) {
	host = os.Getenv("CLICKHOUSE_HOST")
	db = os.Getenv("CLICKHOUSE_DATABASE")
	user = os.Getenv("CLICKHOUSE_USER")
	password = os.Getenv("CLICKHOUSE_PASSWORD")
	if host == "" {
		host = defaultHost
	}
	if db == "" {
		db = defaultDb
	}
	if user == "" {
		user = defaultUser
	}
	return host, db, user, password
}

func useOptions() *clickhouse.Options {
	host, db, user, password := useCredentials()
	return &clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{
			Database: db,
			Username: user,
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
	}
}

func useClickhousePool(ctx context.Context) (driver.Conn, cx.Clickhouse, error) {
	nativeClickhouse, conn, err := cxnative.NewClickhouse(ctx, useOptions())
	if err != nil {
		return nil, nil, err
	}
	return conn, nativeClickhouse, nil
}

func useClickhouseSQLPool(ctx context.Context) (*sql.DB, cx.Clickhouse, error) {
	sqlClickhouse, conn, err := cxsql.NewClickhouse(ctx, useOptions(), &cxsql.RuntimeOptions{})
	if err != nil {
		return nil, nil, err
	}
	return conn, sqlClickhouse, nil
}

func useCommonClient(ctx context.Context, ch cx.Clickhouse) clickhousebuffer.Client {
	return clickhousebuffer.NewClientWithOptions(ctx, ch,
		clickhousebuffer.DefaultOptions().SetFlushInterval(500).SetBatchSize(6),
	)
}

func useClientAndRedisBuffer(
	ctx context.Context,
	ch cx.Clickhouse,
	db *redis.Client,
) (
	clickhousebuffer.Client,
	cx.Buffer,
	error,
) {
	client := useCommonClient(ctx, ch)
	buf, err := cxredis.NewBuffer(ctx, db, "bucket", client.Options().BatchSize())
	if err != nil {
		return nil, nil, fmt.Errorf("could't create redis buffer: %s", err)
	}
	return client, buf, nil
}

func useWriteAPI(client clickhousebuffer.Client, buf cx.Buffer) clickhousebuffer.Writer {
	writeAPI := client.Writer(cx.NewView(integrationTableName, []string{"id", "uuid", "insert_ts"}), buf)
	return writeAPI
}

func writeDataToBuffer(writeAPI clickhousebuffer.Writer) {
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

func writeDataToBufferSafe(writeAPI clickhousebuffer.Writer) {
	writeAPI.TryWriteRow(integrationRow{
		id: 1, uuid: "1", insertTS: time.Now(),
	})
	writeAPI.TryWriteRow(integrationRow{
		id: 2, uuid: "2", insertTS: time.Now(),
	})
	writeAPI.TryWriteRow(integrationRow{
		id: 3, uuid: "3", insertTS: time.Now(),
	})
	writeAPI.TryWriteRow(integrationRow{
		id: 4, uuid: "4", insertTS: time.Now(),
	})
	writeAPI.TryWriteRow(integrationRow{
		id: 5, uuid: "5", insertTS: time.Now(),
	})
	// wait a bit
	<-time.After(50 * time.Millisecond)
}

func checksBuffer(buf cx.Buffer) error {
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

func checkData(values []clickhouseRowData) error {
	log.Printf("received values from clickhouse table: %v", values)
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

func checksClickhouse(ctx context.Context, conn driver.Conn) error {
	// check data in clickhouse, write after flushing
	values, err := fetchClickhouseRows(ctx, conn)
	if err != nil {
		return fmt.Errorf("could't fetch data from clickhouse: %s", err)
	}
	return checkData(values)
}

func checksClickhouseSQL(ctx context.Context, conn *sql.DB) error {
	// check data in clickhouse, write after flushing
	values, err := fetchClickhouseRowsSQL(ctx, conn)
	if err != nil {
		return fmt.Errorf("could't fetch data from clickhouse: %s", err)
	}
	return checkData(values)
}

func TestSomething(t *testing.T) {
	// db.Query()
}

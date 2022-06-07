package native

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/zikwall/clickhouse-buffer/v2/database"
	"github.com/zikwall/clickhouse-buffer/v2/src/buffer"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type clickhouseNative struct {
	conn          driver.Conn
	insertTimeout time.Duration
}

// creates a template for preparing the query
func nativeInsertQuery(table string, cols []string) string {
	prepared := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(cols, ", "))
	return prepared
}

func (c *clickhouseNative) Insert(ctx context.Context, view database.View, rows []buffer.RowSlice) (uint64, error) {
	var err error
	timeoutContext, cancel := context.WithTimeout(ctx, c.insertTimeout)
	defer cancel()
	batch, err := c.conn.PrepareBatch(timeoutContext, nativeInsertQuery(view.Name, view.Columns))
	if err != nil {
		return 0, err
	}
	var affected uint64
	for _, row := range rows {
		if err := batch.Append(row...); err != nil {
			log.Println(err)
		} else {
			affected++
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return affected, nil
}

func (c *clickhouseNative) Close() error {
	return c.conn.Close()
}

func NewClickhouse(ctx context.Context, options *clickhouse.Options) (database.Clickhouse, driver.Conn, error) {
	if options.MaxIdleConns == 0 {
		options.MaxIdleConns = database.GetDefaultMaxIdleConns()
	}
	if options.MaxOpenConns == 0 {
		options.MaxOpenConns = database.GetDefaultMaxOpenConns()
	}
	if options.ConnMaxLifetime == 0 {
		options.ConnMaxLifetime = database.GetDefaultConnMaxLifetime()
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, nil, err
	}
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, nil, err
	}
	return &clickhouseNative{
		conn:          conn,
		insertTimeout: database.GetDefaultInsertDurationTimeout(),
	}, conn, nil
}

func NewClickhouseWithConn(conn driver.Conn) database.Clickhouse {
	return &clickhouseNative{
		conn:          conn,
		insertTimeout: database.GetDefaultInsertDurationTimeout(),
	}
}

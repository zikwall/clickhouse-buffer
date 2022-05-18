package clickhousebuffer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/zikwall/clickhouse-buffer/src/buffer"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Clickhouse interface {
	Insert(context.Context, View, []buffer.RowSlice) (uint64, error)
	Conn() driver.Conn
	Close() error
}

type clickhouseNative struct {
	conn          driver.Conn
	insertTimeout time.Duration
}

func (c *clickhouseNative) Insert(ctx context.Context, view View, rows []buffer.RowSlice) (uint64, error) {
	var err error
	timeoutContext, cancel := context.WithTimeout(ctx, c.insertTimeout)
	defer cancel()
	batch, err := c.conn.PrepareBatch(timeoutContext, nativeInsertQuery(view.Name, view.Columns))
	if err != nil {
		return 0, err
	}
	var affected uint64
	for _, row := range rows {
		if err = batch.Append(row...); err != nil {
			log.Println(err)
		} else {
			affected++
		}
	}
	if err = batch.Send(); err != nil {
		return 0, err
	}
	return affected, nil
}

// creates a template for preparing the query
func nativeInsertQuery(table string, cols []string) string {
	prepared := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(cols, ", "))
	return prepared
}

func (c *clickhouseNative) Conn() driver.Conn {
	return c.conn
}

func (c *clickhouseNative) Close() error {
	return c.conn.Close()
}

func NewNativeClickhouse(ctx context.Context, options *clickhouse.Options) (Clickhouse, error) {
	if options.MaxIdleConns == 0 {
		options.MaxIdleConns = defaultMaxIdleConns
	}
	if options.MaxOpenConns == 0 {
		options.MaxOpenConns = defaultMaxOpenConns
	}
	if options.ConnMaxLifetime == 0 {
		options.ConnMaxLifetime = defaultConnMaxLifetime
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return &clickhouseNative{
		conn:          conn,
		insertTimeout: defaultInsertDurationTimeout,
	}, nil
}

func NewNativeClickhouseWithConn(conn driver.Conn) Clickhouse {
	return &clickhouseNative{
		conn:          conn,
		insertTimeout: defaultInsertDurationTimeout,
	}
}

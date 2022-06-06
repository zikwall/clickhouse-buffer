package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/zikwall/clickhouse-buffer/database"
	"github.com/zikwall/clickhouse-buffer/src/buffer"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type clickhouseSQL struct {
	conn          *sql.DB
	insertTimeout time.Duration
}

func (c *clickhouseSQL) Close() error {
	return c.conn.Close()
}

// creates a template for preparing the query
func insertQuery(table string, cols []string) string {
	prepared := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(cols, ", "))
	return prepared
}

// Insert Currently, the client library does not support the JSONEachRow format, only native byte blocks
// There is no support for user interfaces as well as simple execution of an already prepared request
// The entire batch bid is implemented through so-called "transactions",
// although Clickhouse does not support them - it is only a client solution for preparing requests
func (c *clickhouseSQL) Insert(ctx context.Context, view database.View, rows []buffer.RowSlice) (uint64, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(insertQuery(view.Name, view.Columns))
	if err != nil {
		// If you do not call the rollback function there will be a memory leak and goroutine
		// Such a leak can occur if there is no access to the table or there is no table itself
		if err := tx.Rollback(); err != nil {
			log.Println(err)
		}
		return 0, err
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			log.Println(err)
		}
	}()

	timeoutContext, cancel := context.WithTimeout(ctx, c.insertTimeout)
	defer cancel()

	var affected uint64
	for _, row := range rows {
		// row affected is not supported
		if _, err := stmt.ExecContext(timeoutContext, row...); err == nil {
			affected++
		} else {
			log.Println(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return affected, nil
}

func NewClickhouse(ctx context.Context, options *clickhouse.Options) (database.Clickhouse, *sql.DB, error) {
	if options.MaxIdleConns == 0 {
		options.MaxIdleConns = database.GetDefaultMaxIdleConns()
	}
	if options.MaxOpenConns == 0 {
		options.MaxOpenConns = database.GetDefaultMaxOpenConns()
	}
	if options.ConnMaxLifetime == 0 {
		options.ConnMaxLifetime = database.GetDefaultConnMaxLifetime()
	}

	conn := clickhouse.OpenDB(options)
	conn.SetMaxIdleConns(options.MaxIdleConns)
	conn.SetMaxOpenConns(options.MaxOpenConns)
	conn.SetConnMaxLifetime(options.ConnMaxLifetime)

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.PingContext(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, nil, err
	}
	return &clickhouseSQL{
		conn:          conn,
		insertTimeout: database.GetDefaultInsertDurationTimeout(),
	}, conn, nil
}

func NewClickhouseWithConn(conn *sql.DB) database.Clickhouse {
	return &clickhouseSQL{
		conn:          conn,
		insertTimeout: database.GetDefaultInsertDurationTimeout(),
	}
}

package clickhousebuffer

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"log"
	"net/url"
	"strings"
	"time"
)

type Clickhouse interface {
	Insert(context.Context, View, []buffer.RowSlice) (uint64, error)
}

type View struct {
	Name    string
	Columns []string
}

type ClickhouseImpl struct {
	db            *sqlx.DB
	insertTimeout uint
}

type ClickhouseCfg struct {
	Address  string
	Password string
	User     string
	Database string
	AltHosts string
	IsDebug  bool
}

func NewClickhouseWithOptions(cfg *ClickhouseCfg) (Clickhouse, error) {
	connectionPool, err := sqlx.Open("clickhouse", buildConnectionString(cfg))
	if err != nil {
		return nil, err
	}

	return NewClickhouseWithSqlx(connectionPool)
}

func NewClickhouseWithSqlx(connectionPool *sqlx.DB) (Clickhouse, error) {
	if err := connectionPool.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, fmt.Errorf("[%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace)
		}

		return nil, err
	}

	return &ClickhouseImpl{
		db:            connectionPool,
		insertTimeout: 15000,
	}, nil
}

func (ci *ClickhouseImpl) SetInsertTimeout(timeout uint) {
	ci.insertTimeout = timeout
}

// Insert Currently, the client library does not support the JSONEachRow format, only native byte blocks
// There is no support for user interfaces as well as simple execution of an already prepared request
// The entire batch bid is implemented through so-called "transactions",
// although Clickhouse does not support them - it is only a client solution for preparing requests
func (ci *ClickhouseImpl) Insert(ctx context.Context, view View, rows []buffer.RowSlice) (uint64, error) {
	tx, err := ci.db.Begin()

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

	timeoutContext, cancel := context.WithTimeout(ctx, time.Duration(ci.insertTimeout)*time.Millisecond)
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

func (ci *ClickhouseImpl) ConnectionPool() *sqlx.DB {
	return ci.db
}

func (ci *ClickhouseImpl) Close() error {
	return ci.db.Close()
}

// creates a template for preparing the query
func insertQuery(table string, cols []string) string {
	placeholders := make([]string, 0, len(cols))

	for range cols {
		placeholders = append(placeholders, "?")
	}

	prepared := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	return prepared
}

func buildConnectionString(c *ClickhouseCfg) string {
	u := url.URL{
		Scheme: "tcp",
		Host:   c.Address + ":9000",
	}

	debug := "false"
	if c.IsDebug {
		debug = "true"
	}

	q := u.Query()
	q.Set("debug", debug)
	q.Set("username", c.User)
	q.Set("password", c.Password)
	q.Set("database", c.Database)

	if len(c.AltHosts) > 0 {
		q.Set("alt_hosts", c.AltHosts)
	}

	u.RawQuery = q.Encode()

	return u.String()
}

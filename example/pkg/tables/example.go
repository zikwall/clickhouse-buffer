package tables

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

type ExampleTable struct {
	ID       int32
	UUID     string
	InsertTS time.Time
}

func (t *ExampleTable) Row() cx.Vector {
	return cx.Vector{t.ID, t.UUID, t.InsertTS.Format(time.RFC822)}
}

func ExampleTableName() string {
	return "default.example"
}

func ExampleTableColumns() []string {
	return []string{"id", "uuid", "insert_ts"}
}

var createTableQuery = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id        	Int32,
			uuid   		String,
			insert_ts   String
		) engine=Memory
`, ExampleTableName())

func CreateTableNative(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx, createTableQuery)
}

func CreateTableSQL(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, createTableQuery)
	return err
}

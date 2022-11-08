package tables

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

type AdvancedTable struct {
	Col1 uint8
	Col2 string
	Col3 string
	Col4 uuid.UUID
	// Map type is not allowed. Set 'allow_experimental_map_type = 1'
	// Col5 map[string]uint8
	Col6  []string
	Col7  []interface{}
	Col8  time.Time
	Col9  string
	Col10 time.Time
	Col11 int8
	Col12 time.Time
	Col13 [][]interface{}
}

func (a *AdvancedTable) Row() cx.Vector {
	return cx.Vector{
		a.Col1, a.Col2, a.Col3, a.Col4, a.Col6, a.Col7, a.Col8, a.Col9, a.Col10, a.Col11, a.Col12, a.Col13,
	}
}

func AdvancedTableName() string {
	return "default.advanced_example"
}

func AdvancedTableColumns() []string {
	return []string{"Col1", "Col2", "Col3", "Col4", "Col6", "Col7", "Col8", "Col9", "Col10", "Col11", "Col12", "Col13"}
}

// nolint:gochecknoglobals // it's OK
// SELECT DISTINCT alias_to
// FROM system.data_type_families
//
// ┌─alias_to────┐
// │             │
// │ IPv6        │
// │ IPv4        │
// │ FixedString │
// │ String      │
// │ Float64     │
// │ UInt8       │
// │ UInt16      │
// │ DateTime    │
// │ Decimal     │
// │ UInt32      │
// │ Int8        │
// │ Int16       │
// │ Int32       │
// │ Int64       │
// │ UInt64      │
// │ Float32     │
// └─────────────┘
//
// 17 rows in set. Elapsed: 0.028 sec.
var createAdvancedTableQuery = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			  Col1 UInt8
			, Col2 String
			, Col3 FixedString(3)
			, Col4 UUID
			, Col6 Array(String)
			, Col7 Tuple(String, UInt8, Array(String), Tuple(DateTime, UInt32))
			, Col8 DateTime
			, Col9 Enum('hello' = 1, 'world' = 2)
			, Col10 DateTime64
			, Col11 Bool
			, Col12 Date
			, Col13 Array(Tuple(String, UInt8, Array(String), Tuple(DateTime, UInt32)))
		) Engine = Memory
`, AdvancedTableName())

func CreateAdvancedTableNative(ctx context.Context, conn driver.Conn) error {
	return conn.Exec(ctx, createAdvancedTableQuery)
}

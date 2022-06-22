package cx

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	defaultMaxIdleConns          = 20
	defaultMaxOpenConns          = 21
	defaultConnMaxLifetime       = time.Minute * 15
	defaultInsertDurationTimeout = time.Millisecond * 15000
)

// GetDefaultMaxIdleConns to get away from this decision in the near future
func GetDefaultMaxIdleConns() int {
	return defaultMaxIdleConns
}

// GetDefaultMaxOpenConns to get away from this decision in the near future
func GetDefaultMaxOpenConns() int {
	return defaultMaxOpenConns
}

// GetDefaultConnMaxLifetime to get away from this decision in the near future
func GetDefaultConnMaxLifetime() time.Duration {
	return defaultConnMaxLifetime
}

// GetDefaultInsertDurationTimeout to get away from this decision in the near future
func GetDefaultInsertDurationTimeout() time.Duration {
	return defaultInsertDurationTimeout
}

// But before that, you need to check error code from Clickhouse,
// this is necessary in order to ensure the finiteness of queue.
// see: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
var noRetryErrors = map[int32]struct{}{
	1:   {}, // UNSUPPORTED_METHOD
	2:   {}, // UNSUPPORTED_PARAMETER
	20:  {}, // NUMBER_OF_COLUMNS_DOESNT_MATCH
	60:  {}, // UNKNOWN_TABLE
	62:  {}, // SYNTAX_ERROR
	80:  {}, // INCORRECT_QUERY
	81:  {}, // UNKNOWN_DATABASE
	108: {}, // NO_DATA_TO_INSERT
	158: {}, // TOO_MANY_ROWS
	161: {}, // TOO_MANY_COLUMNS
	164: {}, // READONLY
	192: {}, // UNKNOWN_USER,
	193: {}, // WRONG_PASSWORD
	195: {}, // IP_ADDRESS_NOT_ALLOWED
	229: {}, // QUERY_IS_TOO_LARGE
	241: {}, // MEMORY_LIMIT_EXCEEDED
	242: {}, // TABLE_IS_READ_ONLY
	291: {}, // DATABASE_ACCESS_DENIED
	372: {}, // SESSION_NOT_FOUND
	373: {}, // SESSION_IS_LOCKED
}

// IsResendAvailable checks whether it is possible to resend undelivered messages to the Clickhouse database
// based on the error received from Clickhouse
func IsResendAvailable(err error) bool {
	var (
		exception *clickhouse.Exception
		ok        bool
	)
	if exception, ok = err.(*clickhouse.Exception); !ok {
		return false
	}
	if _, ok = noRetryErrors[exception.Code]; ok {
		return false
	}
	return true
}

package clickhousebuffer

import "time"

const (
	defaultMaxIdleConns    = 20
	defaultMaxOpenConns    = 21
	defaultConnMaxLifetime = time.Minute * 5
)

type ClickhouseOpt struct {
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
}

type Option func(e *ClickhouseOpt)

// WithMaxIdleConns set `maxIdleConns` to ClickhouseOpt
func WithMaxIdleConns(maxIdleConns int) Option {
	return func(opt *ClickhouseOpt) {
		opt.maxIdleConns = maxIdleConns
	}
}

// WithMaxOpenConns set `maxOpenConns` to ClickhouseOpt
func WithMaxOpenConns(maxOpenConns int) Option {
	return func(opt *ClickhouseOpt) {
		opt.maxOpenConns = maxOpenConns
	}
}

// WithConnMaxLifetime set `maxIdleConns` to ClickhouseOpt
func WithConnMaxLifetime(connMaxLifetime time.Duration) Option {
	return func(opt *ClickhouseOpt) {
		opt.connMaxLifetime = connMaxLifetime
	}
}

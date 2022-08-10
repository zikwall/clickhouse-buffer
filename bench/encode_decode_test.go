package bench

import (
	"testing"
	"time"

	"github.com/zikwall/clickhouse-buffer/v4/src/cx"
)

type row struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (r *row) Row() cx.Vector {
	return cx.Vector{r.id, r.uuid, r.insertTS.Format(time.RFC822)}
}

// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkEncodeRow/1000000-12                100          30361854 ns/op        15847294 B/op     240014 allocs/op
// BenchmarkEncodeRow/100000-12                 100           2946954 ns/op         1584748 B/op      24001 allocs/op
// BenchmarkEncodeRow/10000-12                  100            289346 ns/op          158465 B/op       2400 allocs/op
// BenchmarkEncodeRow/1000-12                   100             31659 ns/op           15857 B/op        240 allocs/op
// BenchmarkEncodeRow/100-12                    100              3089 ns/op            1584 B/op         24 allocs/op
// BenchmarkEncodeRow/10-12                     100               383.0 ns/op           158 B/op          2 allocs/op
// PASS
// ok
// nolint:dupl // it's OK
func BenchmarkEncodeRow(b *testing.B) {
	b.Run("1000000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(1000000, b)
		}
	})
	b.Run("100000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(100000, b)
		}
	})
	b.Run("10000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(10000, b)
		}
	})
	b.Run("1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(1000, b)
		}
	})
	b.Run("100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(100, b)
		}
	})
	b.Run("10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encode(10, b)
		}
	})
}

// goos: linux
// goarch: amd64
// pkg: github.com/zikwall/clickhouse-buffer/v4/bench
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkDecodeRow/100000-12                 100          17739535 ns/op         7653390 B/op     200064 allocs/op
// BenchmarkDecodeRow/10000-12                  100           1867818 ns/op          765345 B/op      20006 allocs/op
// BenchmarkDecodeRow/1000-12                   100            181877 ns/op           76521 B/op       2000 allocs/op
// BenchmarkDecodeRow/100-12                    100             16230 ns/op            7656 B/op        200 allocs/op
// BenchmarkDecodeRow/10-12                     100              1661 ns/op             764 B/op         20 allocs/op
// BenchmarkDecodeRow/1-12                      100               227.0 ns/op            76 B/op          2 allocs/op
// PASS
// ok
// nolint:dupl // it's OK
func BenchmarkDecodeRow(b *testing.B) {
	b.Run("100000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decode(100000, b)
		}
	})
	b.Run("10000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decode(10000, b)
		}
	})
	b.Run("1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decode(1000, b)
		}
	})
	b.Run("100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decode(100, b)
		}
	})
	b.Run("10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decode(10, b)
		}
	})
	b.Run("1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decode(1, b)
		}
	})
}

func encode(x int, b *testing.B) {
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < x; i++ {
		r := row{
			id:       1,
			uuid:     "uuid_here",
			insertTS: now,
		}
		_, _ = r.Row().Encode()
	}
}

func decode(x int, b *testing.B) {
	now := time.Now()
	encodes := make([]cx.VectorDecoded, 0, x)
	for i := 0; i < x; i++ {
		r := row{
			id:       1,
			uuid:     "uuid_here",
			insertTS: now,
		}
		enc, _ := r.Row().Encode()
		encodes = append(encodes, cx.VectorDecoded(enc))
	}
	b.ResetTimer()
	for i := range encodes {
		_, _ = encodes[i].Decode()
	}
}

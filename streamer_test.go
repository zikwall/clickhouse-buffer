package clickhouse_buffer

import (
	"context"
	"fmt"
	"github.com/zikwall/clickhouse-buffer/src/api"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"github.com/zikwall/clickhouse-buffer/src/common"
	"testing"
	"time"
)

type ClickhouseImplMock struct{}

func (ch *ClickhouseImplMock) Insert(ctx context.Context, view api.View, rows []common.Vector) (uint64, error) {
	return 0, nil
}

type ClickhouseImplErrMock struct{}

func (ch *ClickhouseImplErrMock) Insert(ctx context.Context, view api.View, rows []common.Vector) (uint64, error) {
	return 0, fmt.Errorf("test error")
}

type VectorMock struct {
	a int
	b string
	c time.Time
}

func (vm VectorMock) Vector() common.Vector {
	return common.Vector{vm.a, vm.b, vm.c}
}

func TestClientImpl_HandleStream(t *testing.T) {
	tableView := api.View{
		Name:    "test_table",
		Columns: []string{"one", "to", "three"},
	}

	t.Run("it should be correct flush send and data", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		defer func() {
			cancel()
		}()

		client := NewClient(ctx, &ClickhouseImplMock{})
		client.Options().SetFlushInterval(500)
		memoryBuffer := buffer.NewInmemoryBuffer(
			client.Options().BatchSize(),
		)
		writeAPI := client.Writer(tableView, memoryBuffer)

		writeAPI.WriteVector(VectorMock{
			a: 1, b: "2", c: time.Now(),
		})

		writeAPI.WriteVector(VectorMock{
			a: 1, b: "2", c: time.Now().Add(time.Second),
		})

		writeAPI.WriteVector(VectorMock{
			a: 1, b: "2", c: time.Now().Add(time.Second * 2),
		})

		<-time.After(time.Second * 1)

		if memoryBuffer.Len() != 0 {
			t.Fatal("Failed, the buffer was expected to be cleared")
		}
	})
}

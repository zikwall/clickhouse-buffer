package clickhouse_buffer

import (
	"context"
	"fmt"
	"github.com/zikwall/clickhouse-buffer/src/api"
	"github.com/zikwall/clickhouse-buffer/src/buffer"
	"github.com/zikwall/clickhouse-buffer/src/types"
	"testing"
	"time"
)

type ClickhouseImplMock struct{}

func (ch *ClickhouseImplMock) Insert(ctx context.Context, view api.View, rows []types.RowSlice) (uint64, error) {
	return 0, nil
}

type ClickhouseImplErrMock struct{}

func (ch *ClickhouseImplErrMock) Insert(ctx context.Context, view api.View, rows []types.RowSlice) (uint64, error) {
	return 0, fmt.Errorf("test error")
}

type RowMock struct {
	id       int
	uuid     string
	insertTs time.Time
}

func (vm RowMock) Row() types.RowSlice {
	return types.RowSlice{vm.id, vm.uuid, vm.insertTs}
}

func TestClientImpl_HandleStream(t *testing.T) {
	tableView := api.View{
		Name:    "test_db.test_table",
		Columns: []string{"id", "uuid", "insert_ts"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		cancel()
	}()

	t.Run("it should be correct send and flush data", func(t *testing.T) {
		client := NewClientWithOptions(ctx, &ClickhouseImplMock{},
			api.DefaultOptions().SetFlushInterval(200).SetBatchSize(3),
		)

		defer client.Close()

		memoryBuffer := buffer.NewInmemoryBuffer(
			client.Options().BatchSize(),
		)

		writeAPI := client.Writer(tableView, memoryBuffer)
		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTs: time.Now(),
		})
		writeAPI.WriteRow(RowMock{
			id: 2, uuid: "2", insertTs: time.Now().Add(time.Second),
		})
		writeAPI.WriteRow(RowMock{
			id: 3, uuid: "3", insertTs: time.Now().Add(time.Second * 2),
		})

		<-time.After(time.Millisecond * 550)

		if memoryBuffer.Len() != 0 {
			t.Fatal("Failed, the buffer was expected to be cleared")
		}
	})

	t.Run("it should be successfully received three errors about writing", func(t *testing.T) {
		client := NewClientWithOptions(ctx, &ClickhouseImplErrMock{},
			api.DefaultOptions().SetFlushInterval(10).SetBatchSize(1),
		)

		defer client.Close()

		memoryBuffer := buffer.NewInmemoryBuffer(
			client.Options().BatchSize(),
		)

		writeAPI := client.Writer(tableView, memoryBuffer)

		var errors []error
		errorsCh := writeAPI.Errors()
		// Create go proc for reading and storing errors
		go func() {
			for err := range errorsCh {
				errors = append(errors, err)
			}
		}()

		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTs: time.Now(),
		})
		writeAPI.WriteRow(RowMock{
			id: 2, uuid: "2", insertTs: time.Now().Add(time.Second),
		})
		writeAPI.WriteRow(RowMock{
			id: 3, uuid: "3", insertTs: time.Now().Add(time.Second * 2),
		})

		<-time.After(time.Millisecond * 50)

		if len(errors) != 3 {
			t.Fatalf("Failed, expected to get three errors, received %d", len(errors))
		}

		if memoryBuffer.Len() != 0 {
			t.Fatal("Failed, the buffer was expected to be cleared")
		}
	})
}

func TestClientImpl_WriteBatch(t *testing.T) {
	tableView := api.View{
		Name:    "test_db.test_table",
		Columns: []string{"id", "uuid", "insert_ts"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		cancel()
	}()

	t.Run("it should be correct send data", func(t *testing.T) {
		client := NewClientWithOptions(ctx, &ClickhouseImplMock{},
			api.DefaultOptions().SetFlushInterval(10).SetBatchSize(1),
		)

		defer client.Close()

		writerBlocking := client.WriterBlocking(tableView)

		err := writerBlocking.WriteRow(ctx, []types.Rower{
			RowMock{
				id: 1, uuid: "1", insertTs: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTs: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTs: time.Now(),
			},
		}...)

		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it should be successfully received error about writing", func(t *testing.T) {
		client := NewClientWithOptions(ctx, &ClickhouseImplErrMock{},
			api.DefaultOptions().SetFlushInterval(10).SetBatchSize(1),
		)

		defer client.Close()

		writerBlocking := client.WriterBlocking(tableView)

		err := writerBlocking.WriteRow(ctx, []types.Rower{
			RowMock{
				id: 1, uuid: "1", insertTs: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTs: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTs: time.Now(),
			},
		}...)

		if err == nil {
			t.Fatal("Failed, expected to get write error, give nil")
		}
	})
}

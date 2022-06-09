package clickhousebuffer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/zikwall/clickhouse-buffer/v2/src/buffer/cxsyncmem"
	"github.com/zikwall/clickhouse-buffer/v2/src/cx"
)

var (
	errClickhouseUnknownException = &clickhouse.Exception{
		Code:       1002,
		Name:       "UNKNOWN_EXCEPTION",
		Message:    "UNKNOWN_EXCEPTION",
		StackTrace: "UNKNOWN_EXCEPTION == UNKNOWN_EXCEPTION",
	}
	errClickhouseUnknownTableException = &clickhouse.Exception{
		Code:       60,
		Name:       "UNKNOWN_TABLE",
		Message:    "UNKNOWN_TABLE",
		StackTrace: "UNKNOWN_TABLE == UNKNOWN_TABLE",
	}
)

type ClickhouseImplMock struct{}

func (c *ClickhouseImplMock) Insert(_ context.Context, _ cx.View, _ []cx.Vector) (uint64, error) {
	return 0, nil
}

func (c *ClickhouseImplMock) Close() error {
	return nil
}

func (c *ClickhouseImplMock) Conn() driver.Conn {
	return nil
}

type ClickhouseImplErrMock struct{}

func (ce *ClickhouseImplErrMock) Insert(_ context.Context, _ cx.View, _ []cx.Vector) (uint64, error) {
	return 0, errClickhouseUnknownException
}

func (ce *ClickhouseImplErrMock) Close() error {
	return nil
}

func (ce *ClickhouseImplErrMock) Conn() driver.Conn {
	return nil
}

type ClickhouseImplErrMockFailed struct{}

func (ce *ClickhouseImplErrMockFailed) Insert(_ context.Context, _ cx.View, _ []cx.Vector) (uint64, error) {
	return 0, errClickhouseUnknownTableException
}

func (ce *ClickhouseImplErrMockFailed) Close() error {
	return nil
}

func (ce *ClickhouseImplErrMockFailed) Conn() driver.Conn {
	return nil
}

type ClickhouseImplRetryMock struct {
	hasErr int32
}

func (cr *ClickhouseImplRetryMock) Insert(_ context.Context, _ cx.View, _ []cx.Vector) (uint64, error) {
	if val := atomic.LoadInt32(&cr.hasErr); val == 0 {
		return 0, errClickhouseUnknownException
	}
	return 1, nil
}

func (cr *ClickhouseImplRetryMock) Close() error {
	return nil
}

func (cr *ClickhouseImplRetryMock) Conn() driver.Conn {
	return nil
}

type RowMock struct {
	id       int
	uuid     string
	insertTS time.Time
}

func (vm RowMock) Row() cx.Vector {
	return cx.Vector{vm.id, vm.uuid, vm.insertTS}
}

// nolint:funlen,gocyclo // it's not important here
func TestClient(t *testing.T) {
	tableView := cx.NewView("test_db.test_table", []string{"id", "uuid", "insert_ts"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("it should be correct send and flush data", func(t *testing.T) {
		client := cx.NewClientWithOptions(ctx, &ClickhouseImplMock{},
			cx.DefaultOptions().
				SetFlushInterval(200).
				SetBatchSize(3).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		memoryBuffer := cxsyncmem.NewBuffer(
			client.Options().BatchSize(),
		)
		writeAPI := client.Writer(tableView, memoryBuffer)
		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTS: time.Now(),
		})
		writeAPI.WriteRow(RowMock{
			id: 2, uuid: "2", insertTS: time.Now().Add(time.Second),
		})
		writeAPI.WriteRow(RowMock{
			id: 3, uuid: "3", insertTS: time.Now().Add(time.Second * 2),
		})
		simulateWait(time.Millisecond * 550)
		if memoryBuffer.Len() != 0 {
			t.Fatal("Failed, the buffer was expected to be cleared")
		}
		simulateWait(time.Millisecond * 500)
		ok, nook, progress := client.RetryClient().Metrics()
		fmt.Println("#1:", ok, nook, progress)
		if ok != 0 || nook != 0 || progress != 0 {
			t.Fatalf("failed, expect zero successful and zero fail retries, expect %d and failed %d", ok, nook)
		}
	})

	// nolint:dupl // it's not important here
	t.Run("it should be successfully received three errors about writing", func(t *testing.T) {
		client := cx.NewClientWithOptions(ctx, &ClickhouseImplErrMock{},
			cx.DefaultOptions().
				SetFlushInterval(10).
				SetBatchSize(1).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		memoryBuffer := cxsyncmem.NewBuffer(
			client.Options().BatchSize(),
		)
		writeAPI := client.Writer(tableView, memoryBuffer)
		var errors []error
		mu := &sync.RWMutex{}
		errorsCh := writeAPI.Errors()
		// Create go proc for reading and storing errors
		go func() {
			for err := range errorsCh {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
		}()
		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTS: time.Now(),
		})
		writeAPI.WriteRow(RowMock{
			id: 2, uuid: "2", insertTS: time.Now().Add(time.Second),
		})
		writeAPI.WriteRow(RowMock{
			id: 3, uuid: "3", insertTS: time.Now().Add(time.Second * 2),
		})
		simulateWait(time.Millisecond * 150)
		mu.RLock()
		defer mu.RUnlock()
		if len(errors) != 3 {
			t.Fatalf("failed, expected to get three errors, received %d", len(errors))
		}
		if memoryBuffer.Len() != 0 {
			t.Fatal("failed, the buffer was expected to be cleared")
		}
		simulateWait(time.Millisecond * 5000)
		ok, nook, progress := client.RetryClient().Metrics()
		fmt.Println("#2:", ok, nook, progress)
		if ok != 0 || nook != 3 || progress != 0 {
			t.Fatalf("failed, expect zero successful and three fail retries, expect %d and failed %d", ok, nook)
		}
	})

	t.Run("it should be successfully handle retry", func(t *testing.T) {
		mock := &ClickhouseImplRetryMock{}
		client := cx.NewClientWithOptions(ctx, mock,
			cx.DefaultOptions().
				SetFlushInterval(10).
				SetBatchSize(1).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		memoryBuffer := cxsyncmem.NewBuffer(
			client.Options().BatchSize(),
		)
		writeAPI := client.Writer(tableView, memoryBuffer)
		var errors []error
		mu := &sync.RWMutex{}
		errorsCh := writeAPI.Errors()
		// Create go proc for reading and storing errors
		go func() {
			for err := range errorsCh {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
		}()
		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTS: time.Now(),
		})
		simulateWait(time.Millisecond * 10)
		atomic.StoreInt32(&mock.hasErr, 1)
		simulateWait(time.Millisecond * 2000)
		mu.RLock()
		defer mu.RUnlock()
		if len(errors) != 1 {
			t.Fatalf("failed, expected to get one error, received %d", len(errors))
		}
		if memoryBuffer.Len() != 0 {
			t.Fatal("failed, the buffer was expected to be cleared")
		}
		ok, nook, progress := client.RetryClient().Metrics()
		fmt.Println("#3:", ok, nook, progress)
		if ok != 1 || nook != 0 || progress != 0 {
			t.Fatalf("failed, expect one successful and zero fail retries, expect %d and failed %d", ok, nook)
		}
		simulateWait(time.Millisecond * 350)
	})

	t.Run("it should be successfully handle retry without error channel", func(t *testing.T) {
		mock := &ClickhouseImplRetryMock{}
		client := cx.NewClientWithOptions(ctx, mock,
			cx.DefaultOptions().
				SetFlushInterval(10).
				SetBatchSize(1).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		memoryBuffer := cxsyncmem.NewBuffer(
			client.Options().BatchSize(),
		)
		writeAPI := client.Writer(tableView, memoryBuffer)
		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTS: time.Now(),
		})
		simulateWait(time.Millisecond * 10)
		atomic.StoreInt32(&mock.hasErr, 1)
		simulateWait(time.Millisecond * 2000)
		if memoryBuffer.Len() != 0 {
			t.Fatal("failed, the buffer was expected to be cleared")
		}
		ok, nook, progress := client.RetryClient().Metrics()
		fmt.Println("#3:", ok, nook, progress)
		if ok != 1 || nook != 0 || progress != 0 {
			t.Fatalf("failed, expect one successful and zero fail retries, expect %d and failed %d", ok, nook)
		}
		simulateWait(time.Millisecond * 350)
	})

	// nolint:dupl // it's not important here
	t.Run("it should be successfully broken retry", func(t *testing.T) {
		client := cx.NewClientWithOptions(ctx, &ClickhouseImplErrMockFailed{},
			cx.DefaultOptions().
				SetFlushInterval(10).
				SetBatchSize(1).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		memoryBuffer := cxsyncmem.NewBuffer(
			client.Options().BatchSize(),
		)
		writeAPI := client.Writer(tableView, memoryBuffer)
		var errors []error
		mu := &sync.RWMutex{}
		errorsCh := writeAPI.Errors()
		// Create go proc for reading and storing errors
		go func() {
			for err := range errorsCh {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
		}()
		writeAPI.WriteRow(RowMock{
			id: 1, uuid: "1", insertTS: time.Now(),
		})
		writeAPI.WriteRow(RowMock{
			id: 2, uuid: "2", insertTS: time.Now().Add(time.Second),
		})
		writeAPI.WriteRow(RowMock{
			id: 3, uuid: "3", insertTS: time.Now().Add(time.Second * 2),
		})
		simulateWait(time.Millisecond * 150)
		mu.RLock()
		defer mu.RUnlock()
		if len(errors) != 3 {
			t.Fatalf("failed, expected to get three errors, received %d", len(errors))
		}
		if memoryBuffer.Len() != 0 {
			t.Fatal("failed, the buffer was expected to be cleared")
		}
		simulateWait(time.Millisecond * 5000)
		ok, nook, progress := client.RetryClient().Metrics()
		fmt.Println("#4:", ok, nook, progress)
		if ok != 0 || nook != 0 || progress != 0 {
			t.Fatalf("failed, expect zero successful and zero fail retries, expect %d and failed %d", ok, nook)
		}
	})
}

func TestClientImplWriteBatch(t *testing.T) {
	tableView := cx.NewView("test_db.test_table", []string{"id", "uuid", "insert_ts"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("it should be correct send data", func(t *testing.T) {
		client := cx.NewClientWithOptions(ctx, &ClickhouseImplMock{},
			cx.DefaultOptions().
				SetFlushInterval(10).
				SetBatchSize(1).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		writerBlocking := client.WriterBlocking(tableView)
		err := writerBlocking.WriteRow(ctx, []cx.Vectorable{
			RowMock{
				id: 1, uuid: "1", insertTS: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTS: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTS: time.Now(),
			},
		}...)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it should be successfully received error about writing", func(t *testing.T) {
		client := cx.NewClientWithOptions(ctx, &ClickhouseImplErrMock{},
			cx.DefaultOptions().
				SetFlushInterval(10).
				SetBatchSize(1).
				SetDebugMode(true).
				SetRetryIsEnabled(true),
		)
		defer client.Close()
		writerBlocking := client.WriterBlocking(tableView)
		err := writerBlocking.WriteRow(ctx, []cx.Vectorable{
			RowMock{
				id: 1, uuid: "1", insertTS: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTS: time.Now(),
			},
			RowMock{
				id: 1, uuid: "1", insertTS: time.Now(),
			},
		}...)
		if err == nil {
			t.Fatal("Failed, expected to get write error, give nil")
		}
	})
	simulateWait(time.Millisecond * 500)
}

func simulateWait(wait time.Duration) {
	<-time.After(wait)
}

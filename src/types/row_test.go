package types

import (
	"testing"
	"time"
)

type RowMock struct {
	id       int
	uuid     string
	insertTs time.Time
}

func (vm RowMock) Row() RowSlice {
	return RowSlice{vm.id, vm.uuid, vm.insertTs.Format(time.RFC822)}
}

func TestRow(t *testing.T) {
	t.Run("it should be success encode to string", func(t *testing.T) {
		slice := RowMock{
			id:       1,
			uuid:     "uuid_here",
			insertTs: time.Now(),
		}.Row()

		encoded, err := slice.Encode()

		if err != nil {
			t.Fatal(err)
		}

		v, err := RowDecoded(encoded).Decode()

		if err != nil {
			t.Fatal(err)
		}

		if len(v) != 3 {
			t.Fatal("Failed, expected to get three columns")
		}

		if v[0] != "1" && v[1] != "uuid_here" {
			t.Fatal("Failed, expected to get [0] => '1' and [1] => 'uuid_here'")
		}
	})
}

package cx

import (
	"bytes"
	"context"
	"encoding/gob"
)

type View struct {
	Name    string
	Columns []string
}

func NewView(name string, columns []string) View {
	return View{Name: name, Columns: columns}
}

type Clickhouse interface {
	Insert(context.Context, View, []Vector) (uint64, error)
	Close() error
}

// Buffer it is the interface for creating a data buffer (temporary storage).
// It is enough to implement this interface so that you can use your own temporary storage
type Buffer interface {
	Write(Vector)
	Read() []Vector
	Len() int
	Flush()
}

// Vectorable interface is an assistant in the correct formation of the order of fields in the data
// before sending it to Clickhouse
type Vectorable interface {
	Row() Vector
}

type Vector []interface{}
type VectorDecoded string

// Encode turns the Vector type into an array of bytes.
// This method is used for data serialization and storage in remote buffers, such as redis.Buffer
func (rw Vector) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(rw)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode This method is required to reverse deserialize an array of bytes in a Vector type
func (rd VectorDecoded) Decode() (Vector, error) {
	var v Vector
	err := gob.NewDecoder(bytes.NewReader([]byte(rd))).Decode(&v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

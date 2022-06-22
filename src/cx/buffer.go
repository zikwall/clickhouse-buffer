package cx

import (
	"bytes"
	"encoding/gob"
)

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

// Vector basic structure for writing to is nothing more than a slice of undefined interfaces
type Vector []interface{}

// Encode turns the Vector type into an array of bytes.
// Encode is used for data serialization and storage in remote buffers, such as redis.Buffer
func (v Vector) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// VectorDecoded a type that is a string, but contains a binary data format
type VectorDecoded string

// Decode method is required to reverse deserialize an array of bytes in a Vector type
func (d VectorDecoded) Decode() (Vector, error) {
	var v Vector
	err := gob.NewDecoder(bytes.NewReader([]byte(d))).Decode(&v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

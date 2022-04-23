package buffer

import (
	"bytes"
	"encoding/gob"
)

// Inline interface is an assistant in the correct formation of the order of fields in the data
// before sending it to Clickhouse
type Inline interface {
	Row() RowSlice
}

type RowSlice []interface{}
type RowDecoded string

// Encode turns the RowSlice type into an array of bytes.
// This method is used for data serialization and storage in remote buffers, such as redis.Buffer
func (rw RowSlice) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(rw)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode This method is required to reverse deserialize an array of bytes in a RowSlice type
func (rd RowDecoded) Decode() (RowSlice, error) {
	var v RowSlice
	err := gob.NewDecoder(bytes.NewReader([]byte(rd))).Decode(&v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

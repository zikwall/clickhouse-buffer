package types

import (
	"bytes"
	"encoding/gob"
)

type Rower interface {
	Row() RowSlice
}

type RowSlice []interface{}
type RowDecoded string

func (rw RowSlice) Encode() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(rw)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (rd RowDecoded) Decode() (RowSlice, error) {
	var v RowSlice
	err := gob.NewDecoder(bytes.NewReader([]byte(rd))).Decode(&v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

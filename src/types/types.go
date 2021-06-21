package types

type Rower interface {
	Row() RowSlice
}

type RowSlice []interface{}

package common

type Scalar interface {
	Vector() Vector
}

type Vector []interface{}

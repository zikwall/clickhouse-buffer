package cx

import "sync/atomic"

type Countable interface {
	Inc() uint64
	Dec() uint64
	Val() uint64
}

type uint64Counter struct {
	val uint64
}

func newUint64Counter() Countable {
	return &uint64Counter{}
}

func (u *uint64Counter) Inc() uint64 {
	return atomic.AddUint64(&u.val, 1)
}

func (u *uint64Counter) Dec() uint64 {
	return atomic.AddUint64(&u.val, ^uint64(0))
}

func (u *uint64Counter) Val() uint64 {
	return atomic.LoadUint64(&u.val)
}

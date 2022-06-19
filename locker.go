package lambdag

import (
	"context"
	"sync"
)

type LockerWithError interface {
	LockWithErr(ctx context.Context) (lockGranted bool, err error)
	UnlockWithErr(ctx context.Context) (err error)
}

type LockerWithoutError struct {
	sync.Locker
}

func NewLockerWithoutError(l sync.Locker) LockerWithoutError {
	return LockerWithoutError{Locker: l}
}

func (l LockerWithoutError) LockWithErr(_ context.Context) (bool, error) {
	l.Locker.Lock()
	return true, nil
}
func (l LockerWithoutError) UnlockWithErr(_ context.Context) error {
	l.Locker.Unlock()
	return nil
}

type NopLocker struct{}

func (l NopLocker) LockWithErr(_ context.Context) (bool, error) {
	return true, nil
}
func (l NopLocker) UnlockWithErr(_ context.Context) error {
	return nil
}

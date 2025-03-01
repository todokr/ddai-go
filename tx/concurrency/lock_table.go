package concurrency

import (
	"ddai-go/file"
	"fmt"
	"sync"
	"time"
)

const maxLockTime = 10 * time.Second

var ErrTimeout = fmt.Errorf("timeout")

type LockTable struct {
	locks map[file.BlockID]int
	cond  *sync.Cond
}

func newLockTable() *LockTable {
	return &LockTable{
		locks: make(map[file.BlockID]int),
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

// SLock locks the block for shared access
// If it cannot lock the block within maxLockTime, return ErrTimeout
func (l *LockTable) SLock(blk file.BlockID) error {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	startTime := time.Now()
	for {
		if time.Since(startTime) > maxLockTime {
			return ErrTimeout
		} else if !l.hasXLock(blk) {
			break
		}
		l.waitWithTimeout(maxLockTime)
	}
	l.locks[blk]++
	return nil
}

// XLock locks the block for exclusive access
// If it cannot lock the block within maxLockTime, return ErrTimeout
func (l *LockTable) XLock(blk file.BlockID) error {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	startTime := time.Now()
	for {
		if time.Since(startTime) > maxLockTime {
			return ErrTimeout
		} else if !l.hasOtherSLocks(blk) {
			break
		}
		l.waitWithTimeout(maxLockTime)
	}
	l.locks[blk] = -1
	return nil
}

func (l *LockTable) Unlock(blk file.BlockID) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	if l.locks[blk] > 1 {
		l.locks[blk]--
	} else {
		delete(l.locks, blk)
		l.cond.Broadcast()
	}
}

func (l *LockTable) waitWithTimeout(timeout time.Duration) {
	timer := time.AfterFunc(timeout, func() {
		l.cond.L.Lock()
		defer l.cond.L.Unlock()
		l.cond.Broadcast()
	})
	l.cond.Wait()
	timer.Stop()
}

func (l *LockTable) hasXLock(blk file.BlockID) bool {
	return l.locks[blk] < 0
}

func (l *LockTable) hasOtherSLocks(blk file.BlockID) bool {
	return l.locks[blk] > 1
}

package concurrency

import (
	"ddai-go/file"
	"fmt"
)

var lockTable = newLockTable()

type Manager struct {
	locks map[file.BlockID]string
}

func New() *Manager {
	return &Manager{
		locks: make(map[file.BlockID]string),
	}
}
func (m *Manager) SLock(blk file.BlockID) error {
	if m.locks[blk] != "" {
		return nil
	}
	if err := lockTable.SLock(blk); err != nil {
		return fmt.Errorf("shared lock failed %v: %w", blk, err)
	}
	m.locks[blk] = "S"
	return nil
}

func (m *Manager) XLock(blk file.BlockID) error {
	if m.HasXLock(blk) {
		return nil
	}
	if err := lockTable.SLock(blk); err != nil {
		return fmt.Errorf("shared lock failed %v: %w", blk, err)
	}
	if err := lockTable.XLock(blk); err != nil {
		return fmt.Errorf("exclusive lock failed %v: %w", blk, err)
	}

	m.locks[blk] = "X"
	return nil
}

func (m *Manager) Release() {
	for blk := range m.locks {
		lockTable.Unlock(blk)
	}
	clear(m.locks)
}

func (m *Manager) HasXLock(blk file.BlockID) bool {
	return m.locks[blk] == "X"
}

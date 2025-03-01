package recovery

import (
	"ddai-go/buffer"
	"ddai-go/file"
	"ddai-go/log"
	"fmt"

	stdlog "log"
)

type Transactor interface {
	Pin(blk file.BlockID) error
	SetString(blk file.BlockID, offset int32, val string, logRecord bool) error
	SetInt(blk file.BlockID, offset int32, val int32, logRecord bool) error
	Unpin(blk file.BlockID)
}

type Manager struct {
	logMgr     *log.Manager
	bufferMgr  *buffer.Manager
	transactor Transactor
	txNum      int32
}

// New transaction
func New(logMgr *log.Manager, bufferMgr *buffer.Manager, tx Transactor, txNum int32) *Manager {
	_, err := newStartRecord(txNum).WriteToLog(logMgr)
	if err != nil {
		stdlog.Panicf("newStartRecord: %v", err)
	}
	return &Manager{
		logMgr:     logMgr,
		bufferMgr:  bufferMgr,
		transactor: tx,
		txNum:      txNum,
	}
}

func (m *Manager) Commit() error {
	if err := m.bufferMgr.FlushAll(m.txNum); err != nil {
		return fmt.Errorf("bufferMgr.FlushAll: %v", err)
	}

	lsn, err := newCommitRecord(m.txNum).WriteToLog(m.logMgr)
	if err != nil {
		return fmt.Errorf("newCommitRecord.WriteToLog: %v", err)
	}
	if err := m.logMgr.Flush(lsn); err != nil {
		return fmt.Errorf("logMgr.Flush: %v", err)
	}
	return nil
}

func (m *Manager) Rollback() error {
	if err := m.doRollback(); err != nil {
		return fmt.Errorf("doRollback: %v", err)
	}
	if err := m.bufferMgr.FlushAll(m.txNum); err != nil {
		return fmt.Errorf("bufferMgr.FlushAll: %v", err)
	}
	lsn, err := newRollbackRecord(m.txNum).WriteToLog(m.logMgr)
	if err != nil {
		return fmt.Errorf("newRollbackRecord.WriteToLog: %v", err)
	}
	if err := m.logMgr.Flush(lsn); err != nil {
		return fmt.Errorf("logMgr.Flush: %v", err)
	}
	return nil
}

func (m *Manager) Recover() error {
	if err := m.doRecover(); err != nil {
		return fmt.Errorf("doRecover: %v", err)
	}
	if err := m.bufferMgr.FlushAll(m.txNum); err != nil {
		return fmt.Errorf("bufferMgr.FlushAll: %v", err)
	}
	lsn, err := newCheckPointRecord().WriteToLog(m.logMgr)
	if err != nil {
		return fmt.Errorf("newCheckPointRecord.WriteToLog: %v", err)
	}
	if err := m.logMgr.Flush(lsn); err != nil {
		return fmt.Errorf("logMgr.Flush: %v", err)
	}
	return nil
}

func (m *Manager) SetInt(buf *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	oldVal := buf.Contents.GetInt(offset)
	blk := buf.Block
	return newSetIntRecord(m.txNum, blk, offset, oldVal).WriteToLog(m.logMgr)
}

func (m *Manager) SetString(buf *buffer.Buffer, offset int32, newVal string) (int32, error) {
	oldVal := buf.Contents.GetString(offset)
	blk := buf.Block
	return newSetStringRecord(m.txNum, blk, offset, oldVal).WriteToLog(m.logMgr)
}

func (m *Manager) doRollback() error {
	iter, err := m.logMgr.Iterator()
	if err != nil {
		return fmt.Errorf("logMgr.Iterator: %v", err)
	}
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return fmt.Errorf("iter.Next: %v", err)
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return fmt.Errorf("NewLogRecord: %v", err)
		}
		if rec.TxNumber() == m.txNum {
			if rec.Op() == Start {
				return nil
			}
			if err := rec.Undo(m.transactor); err != nil {
				return fmt.Errorf("rec.Undo: %v", err)
			}
		}
	}
	return nil
}

func (m *Manager) doRecover() error {
	finishedTx := make(map[int32]any)
	it, err := m.logMgr.Iterator()
	if err != nil {
		return fmt.Errorf("recovery.doRecover: %w", err)
	}
	for it.HasNext() {
		bytes, err := it.Next()
		if err != nil {
			return fmt.Errorf("recovery.doRecover: %w", err)
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return fmt.Errorf("recovery.doRecover for %s: %w", string(bytes), err)
		}
		if rec.Op() == CheckPoint {
			return nil
		} else if rec.Op() == Commit || rec.Op() == Rollback {
			finishedTx[rec.TxNumber()] = struct{}{}
		} else if _, ok := finishedTx[rec.TxNumber()]; !ok {
			if err := rec.Undo(m.transactor); err != nil {
				return fmt.Errorf("undo: %w", err)
			}
		}
	}
	return nil
}

package recovery

import (
	"ddai-go/buffer"
	"ddai-go/file"
	"ddai-go/log"

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

func (m *Manager) Commit() {
	_, err := newCommitRecord(m.txNum).WriteToLog(m.logMgr)
	if err != nil {
		stdlog.Panicf("newCommitRecord: %v", err)
	}
}

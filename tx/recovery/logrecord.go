package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
	"fmt"
)

type LogRecordType = int32

const (
	Undefined LogRecordType = iota
	// Checkpoint records are added to the log in order to reduce the portion of the log
	// that the recovery algorithm needs to consider.
	Checkpoint
	Start
	Commit
	Rollback
	SetInt
	SetString
)

type LogRecord interface {
	Op() LogRecordType
	TxNumber() int32
	Undo(transactor Transactor)
	String() string
	WriteToLog(lm *log.Manager) (int32, error)
}

func NewLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageWith(bytes)
	switch LogRecordType(p.GetInt(0)) {
	case Checkpoint:
		return newCheckPointRecord(), nil
	case Start:
		return newStartRecordFrom(p), nil
	case Commit:
		return newCommitRecordFrom(p), nil
	case Rollback:
		return newRollbackRecordFrom(p), nil
	case SetInt:
		return newSetIntRecordFrom(p), nil
	case SetString:
		return newSetStringRecordFrom(p), nil
	default:
		return nil, fmt.Errorf("unknown LogRecordType: %v", p.GetInt(0))
	}
}

package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
	"ddai-go/tx"
	"fmt"
)

type rollbackRecord struct {
	txNum int32
}

func newRollbackRecord(txNum int32) *rollbackRecord {
	return &rollbackRecord{
		txNum: txNum,
	}
}

func newRollbackRecordFrom(p *file.Page) *rollbackRecord {
	return newRollbackRecord(p.GetInt(file.Int32ByteSize))
}

func (r rollbackRecord) Op() LogRecordType {
	return Rollback
}

func (r rollbackRecord) TxNumber() int32 {
	return r.txNum
}

func (r rollbackRecord) Undo(tx tx.Transaction) {
	// no need to undo rollback itself
}

func (r rollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
}

func (r rollbackRecord) WriteToLog(lm *log.Manager) (int32, error) {
	// 4 bytes for log record type, 4 bytes for transaction number
	typeOffset := file.Int32ByteSize
	buf := make([]byte, typeOffset+file.Int32ByteSize)
	p := file.NewPageWith(buf)
	p.SetInt(0, Rollback)
	p.SetInt(typeOffset, r.txNum)
	return lm.Append(buf)
}

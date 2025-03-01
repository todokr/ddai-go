package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
	"fmt"
)

type commitRecord struct {
	txNum int32
}

func newCommitRecord(txNum int32) *commitRecord {
	return &commitRecord{
		txNum: txNum,
	}
}

func newCommitRecordFrom(p *file.Page) *commitRecord {
	return newCommitRecord(p.GetInt(file.Int32ByteSize))
}

func (r commitRecord) Op() LogRecordType {
	return Commit
}

func (r commitRecord) TxNumber() int32 {
	return r.txNum
}

func (r commitRecord) Undo(transactor Transactor) error {
	// no need to undo commit itself
	return nil
}

func (r commitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", r.txNum)
}

func (r commitRecord) WriteToLog(lm *log.Manager) (int32, error) {
	// 4 bytes for log record type, 4 bytes for transaction number
	typeOffset := file.Int32ByteSize
	buf := make([]byte, typeOffset+file.Int32ByteSize)
	p := file.NewPageWith(buf)
	p.SetInt(0, Commit)
	p.SetInt(typeOffset, r.txNum)
	return lm.Append(buf)
}

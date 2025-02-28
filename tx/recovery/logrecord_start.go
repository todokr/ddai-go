package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
)

var _ LogRecord = (*startRecord)(nil)

type startRecord struct {
	txNum int32
}

func newStartRecord(txNum int32) *startRecord {
	return &startRecord{txNum: txNum}
}

func newStartRecordFrom(p *file.Page) *startRecord {
	// first 4 bytes indicates the type of log record, so skip it
	return &startRecord{txNum: p.GetInt(file.Int32ByteSize)}
}

func (s startRecord) Op() LogRecordType {
	return Start
}

func (s startRecord) TxNumber() int32 {
	return s.txNum
}

func (s startRecord) Undo(tx Transaction) {
	// no need to undo start
}

func (s startRecord) String() string {
	return "<START " + string(s.txNum) + ">"
}

func (s startRecord) WriteToLog(lm *log.Manager) (int32, error) {
	// 4 bytes for log record type, 4 bytes for transaction number
	buf := make([]byte, file.Int32ByteSize+file.Int32ByteSize)
	p := file.NewPageWith(buf)
	offset := p.SetInt(0, Start)
	p.SetInt(offset, s.txNum)
	return lm.Append(buf)
}

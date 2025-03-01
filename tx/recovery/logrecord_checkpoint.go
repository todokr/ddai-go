package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
)

var _ LogRecord = (*checkPointRecord)(nil)

type checkPointRecord struct{}

func newCheckPointRecord() *checkPointRecord {
	return &checkPointRecord{}
}

func (r *checkPointRecord) Op() LogRecordType {
	return CheckPoint
}

func (r *checkPointRecord) TxNumber() int32 {
	return 0
}

func (r *checkPointRecord) Undo(transactor Transactor) error {
	// no need to undo checkpoint
	return nil
}

func (r *checkPointRecord) String() string {
	return "<CHECKPOINT>"
}

func (r *checkPointRecord) WriteToLog(lm *log.Manager) (int32, error) {
	buf := make([]byte, file.Int32ByteSize)
	p := file.NewPageWith(buf)
	p.SetInt(0, CheckPoint)
	return lm.Append(buf)
}

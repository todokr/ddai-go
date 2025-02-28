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
	return Checkpoint
}

func (r *checkPointRecord) TxNumber() int32 {
	return 0
}

func (r *checkPointRecord) Undo(tx Transaction) {
	// no need to undo checkpoint
}

func (r *checkPointRecord) String() string {
	return "<CHECKPOINT>"
}

func (r *checkPointRecord) WriteToLog(lm *log.Manager) (int32, error) {
	buf := make([]byte, file.Int32ByteSize)
	p := file.NewPageWith(buf)
	p.SetInt(0, Checkpoint)
	return lm.Append(buf)
}

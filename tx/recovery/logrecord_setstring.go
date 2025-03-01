package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
	"fmt"

	stdlog "log"
)

type setStringRecord struct {
	txNum  int32
	offset int32
	val    string
	blk    file.BlockID
}

func newSetStringRecord(txNum int32, blk file.BlockID, offset int32, val string) *setStringRecord {
	return &setStringRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func newSetStringRecordFrom(p *file.Page) *setStringRecord {
	txOffset := file.Int32ByteSize
	txNum := p.GetInt(txOffset)

	fileOffset := txOffset + file.Int32ByteSize
	fileName := p.GetString(fileOffset)
	blkOffset := fileOffset + file.MaxLength(len(fileName))
	blkIndex := p.GetInt(blkOffset)
	blk := file.NewBlockID(fileName, blkIndex)

	oOffset := blkOffset + file.Int32ByteSize
	offset := p.GetInt(oOffset)

	valOffset := oOffset + file.Int32ByteSize
	val := p.GetString(valOffset)

	return &setStringRecord{
		txNum:  txNum,
		blk:    blk,
		offset: offset,
		val:    val,
	}
}

func (r setStringRecord) Op() LogRecordType {
	return SetString
}

func (r setStringRecord) TxNumber() int32 {
	return r.txNum
}

func (r setStringRecord) Undo(transactor Transactor) {
	err := transactor.Pin(r.blk)
	if err != nil {
		stdlog.Panicf("cannot pin block %v: %v", r.blk, err)
	}
}

func (r setStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", r.txNum, r.blk, r.offset, r.val)
}

func (r setStringRecord) WriteToLog(lm *log.Manager) (int32, error) {
	txOffset := file.Int32ByteSize
	fileOffset := txOffset + file.Int32ByteSize
	blkOffset := fileOffset + file.MaxLength(len(r.blk.FileName))
	oOffset := blkOffset + file.Int32ByteSize
	valOffset := oOffset + file.Int32ByteSize
	recLen := valOffset + file.Int32ByteSize

	buf := make([]byte, recLen)
	p := file.NewPageWith(buf)
	p.SetInt(0, SetInt)
	p.SetInt(txOffset, r.txNum)
	p.SetString(fileOffset, r.blk.FileName)
	p.SetInt(blkOffset, r.blk.Index)
	p.SetInt(oOffset, r.offset)
	p.SetString(valOffset, r.val)
	return lm.Append(buf)
}

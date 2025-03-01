package recovery

import (
	"ddai-go/file"
	"ddai-go/log"
	"ddai-go/tx"
	"fmt"
	stdlog "log"
)

type setIntRecord struct {
	txNum  int32
	offset int32
	val    int32
	blk    file.BlockID
}

func newSetIntRecord(txNum int32, blk file.BlockID, offset int32, val int32) *setIntRecord {
	return &setIntRecord{
		txNum:  txNum,
		blk:    blk,
		offset: offset,
		val:    val,
	}
}

func newSetIntRecordFrom(p *file.Page) *setIntRecord {
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
	val := p.GetInt(valOffset)

	return &setIntRecord{
		txNum:  txNum,
		blk:    blk,
		offset: offset,
		val:    val,
	}
}

func (r setIntRecord) Op() LogRecordType {
	return SetInt
}

func (r setIntRecord) TxNumber() int32 {
	return r.txNum
}

func (r setIntRecord) Undo(tx tx.Transaction) {
	err := tx.Pin(r.blk)
	if err != nil {
		stdlog.Panicf("cannot pin block %v: %v", r.blk, err)
	}
	tx.SetInt(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)
}

func (r setIntRecord) WriteToLog(lm *log.Manager) (int32, error) {
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
	p.SetInt(valOffset, r.val)
	return lm.Append(buf)
}

func (r setIntRecord) String() string {
	return fmt.Sprintf("<SETINT %d %v %d %d>", r.txNum, r.blk, r.offset, r.val)
}

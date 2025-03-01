package tx

import (
	"ddai-go/file"
)

type Transaction struct {
	bufs *BufferList
}

func (tx *Transaction) Commit() {
	// TODO
}

func (tx *Transaction) Rollback() {
	// TODO
}

func (tx *Transaction) Recover() {
	// TODO
}

func (tx *Transaction) Pin(blk file.BlockID) error {
	return tx.bufs.pin(blk)
}

func (tx *Transaction) Unpin(blk file.BlockID) {
	tx.bufs.unpin(blk)
}

func (tx *Transaction) GetInt(blk file.BlockID, offset int32) int {
	// TODO
	return 0
}
func (tx *Transaction) SetInt(blk file.BlockID, offset int32, value int32, okToLog bool) {
	// TODO
}

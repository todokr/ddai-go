package tx

import (
	"ddai-go/buffer"
	"ddai-go/file"
	"ddai-go/log"
	"ddai-go/tx/concurrency"
	"ddai-go/tx/recovery"
	"fmt"
	"sync/atomic"
)

type Transaction struct {
	recoveryMgr *recovery.Manager
	concurMgr   *concurrency.Manager
	bufferMgr   *buffer.Manager
	fileMgr     *file.Manager
	txNum       int32
	bufs        *BufferList
}

func New(fileMgr *file.Manager, logMgr *log.Manager, bufManager *buffer.Manager) *Transaction {
	txNum := nextTxNum()
	tx := &Transaction{
		concurMgr: concurrency.New(),
		bufferMgr: bufManager,
		fileMgr:   fileMgr,
		txNum:     txNum,
		bufs:      newBufferList(bufManager),
	}
	tx.recoveryMgr = recovery.New(logMgr, bufManager, tx, txNum)
	return tx
}

var txNum = int32(0)

func nextTxNum() int32 {
	atomic.AddInt32(&txNum, 1)
	return txNum
}

func (tx *Transaction) Commit() error {
	if err := tx.recoveryMgr.Commit(); err != nil {
		return fmt.Errorf("commit tx failed %v", err)
	}
	tx.concurMgr.Release()
	tx.bufs.unpinAll()
	fmt.Printf("Transaction %d committed\n", tx.txNum)
	return nil
}

func (tx *Transaction) Rollback() error {
	if err := tx.recoveryMgr.Rollback(); err != nil {
		return fmt.Errorf("rollback tx failed %v", err)
	}
	tx.concurMgr.Release()
	tx.bufs.unpinAll()
	fmt.Printf("Transaction %d rollbacked\n", tx.txNum)
	return nil
}

func (tx *Transaction) Recover() error {
	if err := tx.bufferMgr.FlushAll(tx.txNum); err != nil {
		return fmt.Errorf("bufferMgr.FlushAll: %v\n", err)
	}
	if err := tx.recoveryMgr.Recover(); err != nil {
		return fmt.Errorf("recoveryMgr.Recover: %v\n", err)
	}
	return nil
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
func (tx *Transaction) SetInt(blk file.BlockID, offset int32, value int32, okToLog bool) error {
	// TODO
	return nil
}

func (tx *Transaction) GetString(blk file.BlockID, offset int32) string {
	// TODO
	return ""
}

func (tx *Transaction) SetString(blk file.BlockID, offset int32, value string, okToLog bool) error {
	// TODO
	return nil
}

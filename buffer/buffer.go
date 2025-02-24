package buffer

import (
	"ddai-go/file"
	"errors"
	"fmt"
)

type Buffer struct {
	fileManager *file.Manager
	Contents    *file.Page
	Block       file.BlockID
	pins        int32
	txNum       int32
	lsn         int32
}

func NewBuffer(fm *file.Manager) *Buffer {
	return &Buffer{
		fileManager: fm,
		txNum:       -1,
		Contents:    file.NewPage(fm.BlockSize),
	}
}

func (b *Buffer) SetModified(txNum int32, lsn int32) {
	b.txNum = txNum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) AssignToBlock(blk file.BlockID) error {
	if err := b.flush(); err != nil {
		return fmt.Errorf("buffer.flush: %w", err)
	}
	b.Block = blk
	if err := b.fileManager.Load(blk, b.Contents); err != nil {
		return fmt.Errorf("file.Load: %w", err)
	}
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.txNum <= 0 {
		return nil
	}
	if err := b.fileManager.Save(b.Block, b.Contents); err != nil {
		return fmt.Errorf("file.Save: %w", err)
	}
	b.txNum = -1
	return nil
}

type Manager struct {
	bufferPool   []*Buffer
	numAvailable int32
}

func NewManager(fm *file.Manager, buffSize int32) *Manager {
	bufferPool := make([]*Buffer, buffSize)
	for i := range bufferPool {
		bufferPool[i] = NewBuffer(fm)
	}

	return &Manager{
		bufferPool:   bufferPool,
		numAvailable: buffSize,
	}
}

func (bm *Manager) FlushAll(txNum int32) error {
	for _, buf := range bm.bufferPool {
		if buf.txNum == txNum {
			if err := buf.flush(); err != nil {
				return fmt.Errorf("buffer.flush: %w", err)
			}
		}
	}
	return nil
}

func (bm *Manager) NumAvailable() int32 {
	return bm.numAvailable
}

var ErrBufferAbort = errors.New("buffer pinning aborted")

func (bm *Manager) Pin(blk file.BlockID) (*Buffer, error) {
	buff, err := bm.tryToPin(blk)
	if err != nil {
		return nil, fmt.Errorf("buffer.tryToPin: %w", err)
	}
	if buff == nil {
		return nil, ErrBufferAbort
	}
	return buff, nil
}

func (bm *Manager) Unpin(buff *Buffer) {
	buff.Unpin()
	if !buff.IsPinned() {
		bm.numAvailable++
		// TODO: notifyAll();
	}
}

func (bm *Manager) tryToPin(blk file.BlockID) (*Buffer, error) {
	var buffer *Buffer
	// find existing buffer
	for _, buf := range bm.bufferPool {
		if b := buf.Block; b == blk {
			buffer = buf
		}
	}
	if buffer == nil {
		// choose unpinned buffer
		for _, buf := range bm.bufferPool {
			if !buf.IsPinned() {
				buffer = buf
			}
		}
		if buffer == nil {
			return nil, nil
		}
		if err := buffer.AssignToBlock(blk); err != nil {
			return nil, fmt.Errorf("buffer.AssignToBlock: %w", err)
		}
	}
	if !buffer.IsPinned() {
		bm.numAvailable--
	}
	buffer.Pin()
	return buffer, nil
}

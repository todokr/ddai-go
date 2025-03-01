package tx

import (
	"ddai-go/buffer"
	"ddai-go/file"
	"log"
)

type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	pins    []file.BlockID
	bm      *buffer.Manager
}

func newBufferList(bm *buffer.Manager) *BufferList {
	return &BufferList{
		buffers: make(map[file.BlockID]*buffer.Buffer),
		pins:    make([]file.BlockID, 0),
		bm:      bm,
	}
}

func (b *BufferList) pin(blk file.BlockID) error {
	buf, err := b.bm.Pin(blk)
	if err != nil {
		return err
	}
	b.buffers[blk] = buf
	b.pins = append(b.pins, blk)
	return nil
}

func (b *BufferList) unpin(blk file.BlockID) {
	buf, ok := b.buffers[blk]
	if !ok {
		log.Panicf("buffer not found %v", blk)
	}
	// unpin the buffer
	b.bm.Unpin(buf)
	// remove from buffers
	delete(b.buffers, blk)
	// remove from pins
	var newPins = make([]file.BlockID, 0, len(b.pins))
	for _, p := range b.pins {
		if p != blk {
			newPins = append(newPins, p)
		}
	}
	b.pins = newPins
}

func (b *BufferList) unpinAll() {
	for _, blk := range b.pins {
		b.unpin(blk)
	}
	b.buffers = make(map[file.BlockID]*buffer.Buffer)
	b.pins = make([]file.BlockID, 0)
}

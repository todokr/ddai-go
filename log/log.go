package log

import (
	"ddai-go/file"
	"fmt"
)

type LogIterator struct {
	fileManager *file.Manager
	blk         file.BlockID
	page        *file.Page
	currentPos  int32
	boundary    int32
}

func NewIterator(fm *file.Manager, blk file.BlockID) (*LogIterator, error) {
	b := make([]byte, fm.BlockSize)
	page := file.NewPageWith(b)

	it := &LogIterator{
		fileManager: fm,
		blk:         blk,
		page:        page,
		currentPos:  0,
		boundary:    0,
	}
	if err := it.moveToBlock(blk); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *LogIterator) moveToBlock(blk file.BlockID) error {
	if err := it.fileManager.Load(blk, it.page); err != nil {
		return err
	}
	it.blk = blk
	it.boundary = it.page.GetInt(0)
	it.currentPos = it.boundary
	return nil
}

func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fileManager.BlockSize || it.blk.Index > 0

}

func (it *LogIterator) Next() []byte {
	if it.currentPos == it.fileManager.BlockSize {
		it.blk = file.NewBlockID(it.blk.FileName, it.blk.Index-1)
		_ = it.moveToBlock(it.blk)
	}
	rec := it.page.GetBytes(it.currentPos)
	it.currentPos += file.Int32ByteSize + int32(len(rec))
	return rec
}

// Manager responsible for writing log records to the log file,
// treats the log as just an ever-increasing sequence of log records.
type Manager struct {
	fileManager  *file.Manager
	logFile      string
	logPage      *file.Page
	currentBlk   file.BlockID
	latestLSN    int
	lastSavedLSN int
}

func NewManager(fileManager *file.Manager, logFile string) (*Manager, error) {
	b := make([]byte, fileManager.BlockSize)
	logPage := file.NewPageWith(b)

	logSize, err := fileManager.Length(logFile)
	if err != nil {
		return nil, fmt.Errorf("fileManager.Length: %w", err)
	}

	lm := &Manager{
		fileManager: fileManager,
		logFile:     logFile,
		logPage:     logPage,
	}

	if logSize == 0 {
		lm.currentBlk, err = lm.extendLogBlock()
		if err != nil {
			return nil, fmt.Errorf("lm.appendNewBlock: %w", err)
		}
	} else {
		lm.currentBlk = file.NewBlockID(logFile, logSize-1)
		err = fileManager.Load(lm.currentBlk, logPage)
		if err != nil {
			return nil, fmt.Errorf("fileManager.Load: %w", err)
		}
	}

	return lm, nil
}

func (lm *Manager) extendLogBlock() (file.BlockID, error) {
	blk, err := lm.fileManager.Extend(lm.logFile)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("fileManager.Extend: %w", err)
	}
	lm.logPage.SetInt(0, lm.fileManager.BlockSize)
	err = lm.fileManager.Save(blk, lm.logPage)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("fileManager.Save: %w", err)
	}
	return blk, nil
}

func (lm *Manager) Flush(lsn int) error {
	if lsn < lm.lastSavedLSN {
		return nil
	}
	if err := lm.flush(); err != nil {
		return fmt.Errorf("lm.flush: %w", err)
	}
	return nil
}

func (lm *Manager) flush() error {
	if err := lm.fileManager.Save(lm.currentBlk, lm.logPage); err != nil {
		return fmt.Errorf("fileManager.Save: %w", err)
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

func (lm *Manager) Iterator() (*LogIterator, error) {
	if err := lm.flush(); err != nil {
		return nil, fmt.Errorf("lm.flush: %w", err)
	}
	return NewIterator(lm.fileManager, lm.currentBlk)
}

func (lm *Manager) Append(rec []byte) (int, error) {
	// boundary contains the offset of the most recently added record.
	// This strategy enables the log iterator to read records in reverse order by reading from left to right.
	boundary := lm.logPage.GetInt(0)
	recSize := int32(len(rec))
	bytesNeeded := recSize + file.Int32ByteSize

	if boundary-bytesNeeded < file.Int32ByteSize {
		fmt.Println("flushing-------")
		// It doesn't fit, so move to next
		if err := lm.flush(); err != nil {
			return 0, fmt.Errorf("lm.flush: %w", err)
		}
		extendedBlk, err := lm.extendLogBlock()
		if err != nil {
			return 0, fmt.Errorf("lm.extendLogBlock: %w", err)
		}
		lm.currentBlk = extendedBlk
		boundary = lm.logPage.GetInt(0)
	}
	recPos := boundary - bytesNeeded
	lm.logPage.SetBytes(recPos, rec)
	lm.logPage.SetInt(0, recPos) // the new boundary

	lm.latestLSN += 1

	return lm.latestLSN, nil
}

package log

import (
	"ddai-go/file"
	"fmt"
)

type LogIterator struct {
	fileManager *file.FileManager
	blk         *file.BlockID
	page        *file.Page
	currentPos  int64
	boundary    int64
}

func NewIterator(fm *file.FileManager, blk *file.BlockID) (*LogIterator, error) {
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

func (it *LogIterator) moveToBlock(blk *file.BlockID) error {
	if err := it.fileManager.Load(blk, it.page); err != nil {
		return err
	}
	it.blk = blk
	it.boundary = int64(it.page.GetInt(0))
	it.currentPos = it.boundary
	return nil
}

func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fileManager.BlockSize || it.blk.Index > 0

}

func (it *LogIterator) Next() []byte {
	if it.currentPos == it.fileManager.BlockSize {
		it.blk = file.NewBlockID(it.blk.FileName, it.blk.Index-1)
		it.moveToBlock(it.blk)
	}
	rec := it.page.GetBytes(int(it.currentPos))
	it.currentPos += int64(file.Int32ByteSize + len(rec))
	return rec
}

// LogManager responsible for writing log records to the log file,
// treats the log as just an ever-increasing sequence of log records.
type LogManager struct {
	fileManager  *file.FileManager
	logFile      string
	logPage      *file.Page
	currentBlk   *file.BlockID
	latestLSN    int
	lastSavedLSN int
}

func NewLogManager(fileManager *file.FileManager, logFile string) (*LogManager, error) {
	b := make([]byte, fileManager.BlockSize)
	logPage := file.NewPageWith(b)

	logSize, err := fileManager.Length(logFile)
	if err != nil {
		return nil, fmt.Errorf("fileManager.Length: %w", err)
	}

	lm := &LogManager{
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

func (lm *LogManager) extendLogBlock() (*file.BlockID, error) {
	blk, err := lm.fileManager.Extend(lm.logFile)
	if err != nil {
		return nil, fmt.Errorf("fileManager.Extend: %w", err)
	}
	lm.logPage.SetInt(0, int32(lm.fileManager.BlockSize))
	err = lm.fileManager.Save(blk, lm.logPage)
	if err != nil {
		return nil, fmt.Errorf("fileManager.Save: %w", err)
	}
	return blk, nil
}

func (lm *LogManager) Flush(lsn int) error {
	if lsn < lm.lastSavedLSN {
		return nil
	}
	if err := lm.flush(); err != nil {
		return fmt.Errorf("lm.flush: %w", err)
	}
	return nil
}

func (lm *LogManager) flush() error {
	if err := lm.fileManager.Save(lm.currentBlk, lm.logPage); err != nil {
		return fmt.Errorf("fileManager.Save: %w", err)
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

func (lm *LogManager) Iterator() (*LogIterator, error) {
	if err := lm.flush(); err != nil {
		return nil, fmt.Errorf("lm.flush: %w", err)
	}
	return NewIterator(lm.fileManager, lm.currentBlk)
}

func (lm *LogManager) Append(rec []byte) (int, error) {
	// boundary contains the offset of the most recently added record.
	// This strategy enables the log iterator to read records in reverse order by reading from left to right.
	boundary := int(lm.logPage.GetInt(0))
	recSize := len(rec)
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
		boundary = int(lm.logPage.GetInt(0))
	}
	recPos := boundary - bytesNeeded
	lm.logPage.SetBytes(recPos, rec)
	lm.logPage.SetInt(0, int32(recPos)) // the new boundary

	lm.latestLSN += 1

	return lm.latestLSN, nil
}

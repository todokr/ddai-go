package server

import (
	"ddai-go/buffer"
	"ddai-go/file"
	"ddai-go/log"
	"fmt"
)

type SimpleDB struct {
	FileManager   *file.Manager
	LogManager    *log.Manager
	BufferManager *buffer.Manager
}

const logFile = "simpledb.log"

func NewSimpleDB(dbDir string, blockSize int32, buffSize int32) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, blockSize)
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	logManager, err := log.NewManager(fileManager, logFile)
	if err != nil {
		return nil, fmt.Errorf("log.NewManager: %w", err)
	}

	bufferManager := buffer.NewManager(fileManager, buffSize)

	return &SimpleDB{fileManager, logManager, bufferManager}, nil
}

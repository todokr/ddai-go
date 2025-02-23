package server

import (
	"ddai-go/file"
	"ddai-go/log"
	"fmt"
)

type SimpleDB struct {
	FileManager *file.FileManager
	LogManager  *log.LogManager
}

const logFile = "simpledb.log"

func NewSimpleDB(dbDir string, blockSize int) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}

	logManager, err := log.NewLogManager(fileManager, logFile)
	if err != nil {
		return nil, fmt.Errorf("log.NewLogManager: %w", err)
	}
	return &SimpleDB{fileManager, logManager}, nil
}

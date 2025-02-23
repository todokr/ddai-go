package server

import (
	"ddai-go/file"
	"fmt"
)

type SimpleDB struct {
	FileManager *file.Manager
}

func NewSimpleDB(dbDir string, blockSize int) (*SimpleDB, error) {
	fileManager, err := file.NewManager(dbDir, int64(blockSize))
	if err != nil {
		return nil, fmt.Errorf("file.NewManager: %w", err)
	}
	return &SimpleDB{fileManager}, nil
}

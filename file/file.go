package file

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"unicode/utf16"
)

type BlockID struct {
	FileName string
	blockNum int64
}

func NewBlockID(filename string, blockNum int64) *BlockID {
	return &BlockID{FileName: filename, blockNum: blockNum}
}

type Page struct {
	buffer []byte
}

const (
	int32ByteSize = 4
	utf16ByteSize = 2
)

func NewPage(blockSize int64) *Page {
	return &Page{
		buffer: make([]byte, blockSize),
	}
}

// SetInt stores an int32 at the specified offset in the page.
// returns byte size that the val occupies, intended to be used for calculating next offset
func (p *Page) SetInt(offset int, val int32) int {
	binary.LittleEndian.PutUint32(
		p.buffer[offset:offset+int32ByteSize],
		uint32(val),
	)
	return int32ByteSize
}

func (p *Page) GetInt(offset int) int32 {
	bytes := p.buffer[offset : offset+int32ByteSize]
	return int32(binary.LittleEndian.Uint32(bytes))
}

// SetBytes stores a byte slice at the specified offset in the page.
// returns byte size thad the val occupies, intended to be used for calculating next offset
func (p *Page) SetBytes(offset int, val []byte) int {
	p.SetInt(offset, int32(len(val)))
	copy(p.buffer[offset+int32ByteSize:], val)
	return int32ByteSize + len(val)
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	from := offset + int32ByteSize // skip int32 representing length
	to := from + int(length)
	return p.buffer[from:to]
}

// SetString stores a string at the specified offset in the page.
// returns byte size that the val occupies, intended to be used for calculating next offset
func (p *Page) SetString(offset int, val string) int {
	runes := utf16.Encode([]rune(val))
	p.SetInt(offset, int32(len(runes))*utf16ByteSize)
	for i, r := range runes {
		from := offset + int32ByteSize + i*utf16ByteSize // skip int32 representing length
		binary.LittleEndian.PutUint16(p.buffer[from:from+utf16ByteSize], r)
	}
	return int32ByteSize + (len(runes) * utf16ByteSize)
}

func (p *Page) GetString(offset int) string {
	length := int(p.GetInt(offset)) / utf16ByteSize
	runes := make([]uint16, length)
	for i := range length {
		from := offset + int32ByteSize + i*utf16ByteSize // skip int32 representing length
		runes[i] = binary.LittleEndian.Uint16(p.buffer[from : from+utf16ByteSize])
	}
	return string(utf16.Decode(runes))
}

type Manager struct {
	DbDir     string
	BlockSize int64
	files     map[string]*os.File
}

func NewManager(dbDir string, blockSize int64) (*Manager, error) {
	// if not exist, create DbDir recursively
	if _, err := os.Stat(dbDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("os.Stat: %w", err)
		}
		err = os.MkdirAll(dbDir, 0o700)
		if err != nil {
			return nil, fmt.Errorf("os.MkdirAll: %w", err)
		}
	}

	// remove any leftover temprary files
	files, err := os.ReadDir(dbDir)
	if err != nil {
		return nil, fmt.Errorf("os.ReadDir: %w", err)
	}
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "temp") {
			continue
		}

		if err = os.Remove(file.Name()); err != nil {
			return nil, fmt.Errorf("os.Remove: %w", err)
		}
	}
	return &Manager{
		DbDir:     dbDir,
		BlockSize: blockSize,
		files:     make(map[string]*os.File),
	}, nil
}

// Load bytes corresponds block ID from disk into a page
func (fm *Manager) Load(blk *BlockID, p *Page) error {
	f, err := fm.open(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(fm.BlockSize*blk.blockNum, 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Read(p.buffer)
	if err != nil {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

// Save the contents of the page to the specified block.
func (fm *Manager) Save(blk *BlockID, p *Page) error {
	f, err := fm.open(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(fm.BlockSize*blk.blockNum, 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Write(p.buffer)
	if err != nil {
		return fmt.Errorf("f.Write: %w", err)
	}

	return nil
}

func (fm *Manager) open(fileName string) (*os.File, error) {
	if f, ok := fm.files[fileName]; ok {
		return f, nil
	}

	f, err := os.OpenFile(path.Join(fm.DbDir, fileName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	fm.files[fileName] = f

	return f, nil
}

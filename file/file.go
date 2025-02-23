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
	Index    int64
}

func NewBlockID(filename string, index int64) *BlockID {
	return &BlockID{FileName: filename, Index: index}
}

type Page struct {
	Buffer []byte
}

const (
	Int32ByteSize = 4
	Utf16ByteSize = 2
)

// NewPage - for creating data buffers
func NewPage(blockSize int64) *Page {
	return &Page{
		Buffer: make([]byte, blockSize),
	}
}

// NewPageWith - for creating log pages
func NewPageWith(buffer []byte) *Page {
	return &Page{
		Buffer: buffer,
	}
}

// SetInt stores an int32 at the specified offset in the page.
// returns byte size that the val occupies, intended to be used for calculating next offset
func (p *Page) SetInt(offset int, val int32) int {
	binary.LittleEndian.PutUint32(
		p.Buffer[offset:offset+Int32ByteSize],
		uint32(val),
	)
	return Int32ByteSize
}

func (p *Page) GetInt(offset int) int32 {
	bytes := p.Buffer[offset : offset+Int32ByteSize]
	return int32(binary.LittleEndian.Uint32(bytes))
}

// SetBytes stores a byte slice at the specified offset in the page.
// returns byte size thad the val occupies, intended to be used for calculating next offset
func (p *Page) SetBytes(offset int, val []byte) int {
	p.SetInt(offset, int32(len(val)))
	copy(p.Buffer[offset+Int32ByteSize:], val)
	return MaxLength(len(val))
}

func (p *Page) GetBytes(offset int) []byte {
	length := p.GetInt(offset)
	from := offset + Int32ByteSize // skip int32 representing length
	to := from + int(length)
	return p.Buffer[from:to]
}

// SetString stores a string at the specified offset in the page.
// returns byte size that the val occupies, intended to be used for calculating next offset
func (p *Page) SetString(offset int, val string) int {
	runes := utf16.Encode([]rune(val))
	p.SetInt(offset, int32(len(runes))*Utf16ByteSize)
	for i, r := range runes {
		from := offset + Int32ByteSize + i*Utf16ByteSize // skip int32 representing length
		binary.LittleEndian.PutUint16(p.Buffer[from:from+Utf16ByteSize], r)
	}
	return MaxLength(len(runes))
}

func (p *Page) GetString(offset int) string {
	length := int(p.GetInt(offset)) / Utf16ByteSize
	runes := make([]uint16, length)
	for i := range length {
		from := offset + Int32ByteSize + i*Utf16ByteSize // skip int32 representing length
		runes[i] = binary.LittleEndian.Uint16(p.Buffer[from : from+Utf16ByteSize])
	}
	return string(utf16.Decode(runes))
}

func MaxLength(length int) int {
	return Int32ByteSize + length*Utf16ByteSize
}

type FileManager struct {
	DbDir     string
	BlockSize int64
	files     map[string]*os.File
}

func NewManager(dbDir string, blockSize int64) (*FileManager, error) {
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
	return &FileManager{
		DbDir:     dbDir,
		BlockSize: blockSize,
		files:     make(map[string]*os.File),
	}, nil
}

// Load bytes corresponds block ID from disk into a page
func (fm *FileManager) Load(blk *BlockID, p *Page) error {
	f, err := fm.open(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(fm.BlockSize*blk.Index, 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Read(p.Buffer)
	if err != nil {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

// Save the contents of the page to the specified block.
func (fm *FileManager) Save(blk *BlockID, p *Page) error {
	f, err := fm.open(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(fm.BlockSize*blk.Index, 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Write(p.Buffer)
	if err != nil {
		return fmt.Errorf("f.Write: %w", err)
	}

	return nil
}

func (fm *FileManager) Extend(filename string) (*BlockID, error) {
	newBlockIndex, err := fm.Length(filename) // Length == Index + 1
	if err != nil {
		return nil, fmt.Errorf("fm.Length: %w", err)
	}
	blk := NewBlockID(filename, newBlockIndex)
	b := make([]byte, fm.BlockSize)

	f, err := fm.open(blk.FileName)
	if err != nil {
		return nil, fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(blk.Index*fm.BlockSize, 0)
	if err != nil {
		return nil, fmt.Errorf("f.Seek: %w", err)
	}
	_, err = f.Write(b)
	if err != nil {
		return nil, fmt.Errorf("f.Write: %w", err)
	}

	return blk, nil
}

// Length returns how many blocks are in the file
func (fm *FileManager) Length(filename string) (int64, error) {
	f, err := fm.open(filename)
	if err != nil {
		return 0, fmt.Errorf("fm.open: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("f.Stat: %w", err)
	}

	return fi.Size() / fm.BlockSize, nil
}

func (fm *FileManager) open(fileName string) (*os.File, error) {
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

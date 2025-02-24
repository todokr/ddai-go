package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"unicode/utf16"
)

type BlockID struct {
	FileName string
	Index    int32
}

func NewBlockID(filename string, index int32) BlockID {
	return BlockID{FileName: filename, Index: index}
}

type Page struct {
	Buffer []byte
}

const (
	Int32ByteSize int32 = 4
	Utf16ByteSize int32 = 2
)

// NewPage - for creating data buffers
func NewPage(blockSize int32) *Page {
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
func (p *Page) SetInt(offset int32, val int32) int32 {
	binary.LittleEndian.PutUint32(
		p.Buffer[offset:offset+Int32ByteSize],
		uint32(val),
	)
	return Int32ByteSize
}

func (p *Page) GetInt(offset int32) int32 {
	bytes := p.Buffer[offset : offset+Int32ByteSize]
	return int32(binary.LittleEndian.Uint32(bytes))
}

// SetBytes stores a byte slice at the specified offset in the page.
// returns byte size thad the val occupies, intended to be used for calculating next offset
func (p *Page) SetBytes(offset int32, val []byte) int32 {
	p.SetInt(offset, int32(len(val)))
	copy(p.Buffer[offset+Int32ByteSize:], val)
	return MaxLength(len(val))
}

func (p *Page) GetBytes(offset int32) []byte {
	length := p.GetInt(offset)
	from := offset + Int32ByteSize // skip int32 representing length
	to := from + length
	return p.Buffer[from:to]
}

// SetString stores a string at the specified offset in the page.
// returns byte size that the val occupies, intended to be used for calculating next offset
func (p *Page) SetString(offset int32, val string) int32 {
	runes := utf16.Encode([]rune(val))
	p.SetInt(offset, int32(len(runes))*Utf16ByteSize)
	for i, r := range runes {
		from := offset + Int32ByteSize + int32(i)*Utf16ByteSize // skip int32 representing length
		binary.LittleEndian.PutUint16(p.Buffer[from:from+Utf16ByteSize], r)
	}
	return MaxLength(len(runes))
}

func (p *Page) GetString(offset int32) string {
	length := p.GetInt(offset) / Utf16ByteSize
	runes := make([]uint16, length)
	for i := range length {
		from := offset + Int32ByteSize + i*Utf16ByteSize // skip int32 representing length
		runes[i] = binary.LittleEndian.Uint16(p.Buffer[from : from+Utf16ByteSize])
	}
	return string(utf16.Decode(runes))
}

func MaxLength(length int) int32 {
	return Int32ByteSize + int32(length)*Utf16ByteSize
}

type Manager struct {
	DbDir     string
	BlockSize int32
	files     map[string]*os.File
}

func NewManager(dbDir string, blockSize int32) (*Manager, error) {
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

	// remove any leftover temp files
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
func (fm *Manager) Load(blk BlockID, p *Page) error {
	f, err := fm.open(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(int64(fm.BlockSize*blk.Index), 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Read(p.Buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

// Save the contents of the page to the specified block.
func (fm *Manager) Save(blk BlockID, p *Page) error {
	f, err := fm.open(blk.FileName)
	if err != nil {
		return fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(int64(fm.BlockSize*blk.Index), 0)
	if err != nil {
		return fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Write(p.Buffer)
	if err != nil {
		return fmt.Errorf("f.Write: %w", err)
	}

	return nil
}

func (fm *Manager) Extend(filename string) (BlockID, error) {
	newBlockIndex, err := fm.Length(filename) // Length == Index + 1
	if err != nil {
		return BlockID{}, fmt.Errorf("fm.Length: %w", err)
	}
	blk := NewBlockID(filename, newBlockIndex)
	b := make([]byte, fm.BlockSize)

	f, err := fm.open(blk.FileName)
	if err != nil {
		return BlockID{}, fmt.Errorf("fm.open: %w", err)
	}

	_, err = f.Seek(int64(fm.BlockSize*blk.Index), 0)
	if err != nil {
		return BlockID{}, fmt.Errorf("f.Seek: %w", err)
	}
	_, err = f.Write(b)
	if err != nil {
		return BlockID{}, fmt.Errorf("f.Write: %w", err)
	}

	return blk, nil
}

// Length returns how many blocks are in the file
func (fm *Manager) Length(filename string) (int32, error) {
	f, err := fm.open(filename)
	if err != nil {
		return 0, fmt.Errorf("fm.open: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("f.Stat: %w", err)
	}

	return int32(fi.Size()) / fm.BlockSize, nil
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

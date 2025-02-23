package file_test

import (
	"ddai-go/file"
	"ddai-go/server"
	"fmt"
	"path"
	"testing"
)

func TestFile(t *testing.T) {
	t.Parallel()

	dbDir := path.Join(t.TempDir(), "filetest")
	db, err := server.NewSimpleDB(dbDir, 400)
	if err != nil {
		t.Fatalf("server.NewSimpleDB: %v", err)
	}

	fm := db.FileManager
	page1 := file.NewPage(fm.BlockSize)

	strPos1 := 0
	inStr1 := "hello"
	strByteSize1 := page1.SetString(strPos1, inStr1)
	fmt.Printf("strByteSize1: %d\n", strByteSize1)

	intPos1 := strPos1 + strByteSize1
	inInt1 := int32(123)
	intByteSize1 := page1.SetInt(intPos1, inInt1)

	strPos2 := intPos1 + intByteSize1
	inStr2 := "world"
	strByteSize2 := page1.SetString(strPos2, inStr2)

	intPos2 := strPos2 + strByteSize2
	inInt2 := int32(456)
	page1.SetInt(intPos2, inInt2)

	blk := file.NewBlockID("testblock", 0)
	err = fm.Save(blk, page1)

	if err != nil {
		t.Fatalf("fm.Save: %v", err)
	}

	page2 := file.NewPage(fm.BlockSize)
	err = fm.Load(blk, page2)
	if err != nil {
		t.Fatalf("fm.Load: %v", err)
	}

	actStr1 := page2.GetString(strPos1)
	if actStr1 != inStr1 {
		t.Errorf("actStr1=%q, want %q", actStr1, inStr1)
	}

	actInt1 := page2.GetInt(intPos1)
	if actInt1 != inInt1 {
		t.Errorf("actInt1=%d, want %d", actInt1, inInt1)
	}

	actStr2 := page2.GetString(strPos2)
	if actStr2 != inStr2 {
		t.Errorf("actStr2=%q, want %q", actStr2, inStr2)
	}

	actInt2 := page2.GetInt(intPos2)
	if actInt2 != inInt2 {
		t.Errorf("actInt2=%d, want %d", actInt2, inInt2)
	}
}

package log_test

import (
	"ddai-go/file"
	"ddai-go/log"
	"ddai-go/server"
	"fmt"
	"path"
	"strconv"
	"testing"
)

func TestLog(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB(path.Join(t.TempDir(), "filetest"), 400, 8)
	if err != nil {
		t.Fatalf("server.NewSimpleDB: %v", err)
	}

	logManager := db.LogManager

	createLogRecord := func(s string, n int) []byte {
		spos := int32(0)
		npos := spos + file.MaxLength(len(s))
		b := make([]byte, npos+file.Int32ByteSize)
		p := file.NewPageWith(b)
		p.SetString(spos, s)
		p.SetInt(npos, int32(n))
		return b
	}

	createRecords := func(start int, end int) {
		fmt.Println("Creating records:")
		for i := start; i <= end; i++ {
			rec := createLogRecord("record"+strconv.Itoa(i), i+100)
			lsn, err := logManager.Append(rec)
			if err != nil {
				t.Fatalf("Append: %v", err)
			}
			fmt.Println(fmt.Sprintf("lsn: %d", lsn))
		}
		fmt.Println("")
	}

	createRecords(1, 35)
	output := peekLogRecords(logManager)
	if want := genWant(35); output != want {
		t.Fatalf("got=%v, want %q", output, want)
	}

	createRecords(36, 70)
	output = peekLogRecords(logManager)
	if want := genWant(70); output != want {
		t.Fatalf("got=%v, want %q", output, want)
	}
}

func peekLogRecords(logManager *log.Manager) string {
	iter, err := logManager.Iterator()
	if err != nil {
		panic(err)
	}

	res := ""
	sentinel := 0
	for iter.HasNext() {
		rec := iter.Next()
		p := file.NewPageWith(rec)
		s := p.GetString(0)
		npos := file.MaxLength(len(s))
		val := p.GetInt(npos)
		output := fmt.Sprintf("[%s, %d]\n", s, val)
		res += output
		sentinel++
		if sentinel > 100 {
			panic("Too many records")
		}
	}
	return res
}

func genWant(n int) string {
	want := ""
	for i := n; i > 0; i-- {
		want += fmt.Sprintf("[%s, %d]\n", "record"+strconv.Itoa(i), i+100)
	}
	return want
}

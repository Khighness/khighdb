package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-11-16

func TestNewDBFile(t *testing.T) {
	path := strings.ReplaceAll("testdata/khighdb", "/", string(os.PathSeparator))
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)

	type args struct {
		path      string
		fileId    uint32
		method    FileRWMethod
		blockSize int64
		eType     uint16
	}
	tests := []struct {
		name    string
		args    args
		want    *DBFile
		wantErr bool
	}{
		{"f-string", args{path: "testdata/khighdb", fileId: 0, method: Standard, blockSize: 1 * 1024 * 1024, eType: String}, nil, false},
		{"f-list", args{path: "testdata/khighdb", fileId: 0, method: Standard, blockSize: 1 * 1024 * 1024, eType: List}, nil, false},
		{"f-hash", args{path: "testdata/khighdb", fileId: 0, method: Standard, blockSize: 1 * 1024 * 1024, eType: Hash}, nil, false},
		{"f-set", args{path: "testdata/khighdb", fileId: 0, method: Standard, blockSize: 1 * 1024 * 1024, eType: Set}, nil, false},
		{"f-zset", args{path: "testdata/khighdb", fileId: 0, method: Standard, blockSize: 1 * 1024 * 1024, eType: ZSet}, nil, false},

		{"m-string", args{path: "testdata/khighdb", fileId: 1, method: MMap, blockSize: 1 * 1024 * 1024, eType: String}, nil, false},
		{"m-list", args{path: "testdata/khighdb", fileId: 2, method: MMap, blockSize: 1 * 1024 * 1024, eType: List}, nil, false},
		{"m-hash", args{path: "testdata/khighdb", fileId: 3, method: MMap, blockSize: 1 * 1024 * 1024, eType: Hash}, nil, false},
		{"m-set", args{path: "testdata/khighdb", fileId: 4, method: MMap, blockSize: 1 * 1024 * 1024, eType: Set}, nil, false},
		{"m-zset", args{path: "testdata/khighdb", fileId: 5, method: MMap, blockSize: 1 * 1024 * 1024, eType: ZSet}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbFile, err := NewDBFile(tt.args.path, tt.args.fileId, tt.args.method, tt.args.blockSize, tt.args.eType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDBFile() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			assert.NotNil(t, dbFile)
		})
	}
}

func TestDBFile_Read(t *testing.T) {
	path := strings.ReplaceAll("testdata/khighdb", "/", string(os.PathSeparator))
	os.MkdirAll(path, os.ModePerm)
	defer os.RemoveAll(path)

	tt := func(method FileRWMethod, fileId uint32) {
		offset := writeForRead(path, method)
		file, err := NewDBFile(path, fileId, method, 1024, String)
		if err != nil {
			panic(err)
		}
		for _, off := range offset {
			e, err := file.Read(off)
			assert.Nil(t, err)
			t.Logf("%s: %+v", e.Meta.Key, e)
		}
	}

	t.Run("standard", func(t *testing.T) {
		tt(Standard, 0)
	})
	t.Run("mmap", func(t *testing.T) {
		tt(MMap, 0)
	})
}

func writeForRead(path string, method FileRWMethod) []int64 {
	deadline := time.Now().Add(time.Second * 100).Unix()
	entries := []*Entry{
		NewEntryWithTxn([]byte("key-1"), []byte("val-1"), []byte("extra-something"), 101, String, 1),
		NewEntryWithoutExtra([]byte("key-2"), []byte("val-2"), String, 0),
		NewEntry([]byte("key-3"), []byte("val-3"), []byte("extra-something"), String, 0),
		NewEntryWithExpire([]byte("key-4"), []byte("val-4"), deadline, String, 0),
	}

	dbFile, err := NewDBFile(path, 0, method, 1024, String)
	if err != nil {
		panic(err)
	}

	var offset []int64
	offset = append(offset, 0)
	for _, e := range entries {
		if err = dbFile.Write(e); err != nil {
			panic(err)
		}
		offset = append(offset, dbFile.Offset)
	}
	return offset[0 : len(offset)-1]
}

func TestBuild(t *testing.T) {
	type args struct {
		path      string
		method    FileRWMethod
		blockSize int64
	}
}

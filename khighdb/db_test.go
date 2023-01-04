package khighdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-29

const (
	alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestOpen(t *testing.T) {
	t.Run("fileio-keyonly", func(t *testing.T) {
		db := newKhighDB(FileIO, KeyOnlyMemMode)
		defer destroyDB(db)
	})

	t.Run("fileio-keyval", func(t *testing.T) {
		db := newKhighDB(FileIO, KeyValueMemMode)
		defer destroyDB(db)
	})

	t.Run("mmap-keyonly", func(t *testing.T) {
		db := newKhighDB(MMap, KeyOnlyMemMode)
		defer destroyDB(db)
	})

	t.Run("mmap-keyval", func(t *testing.T) {
		db := newKhighDB(MMap, KeyValueMemMode)
		defer destroyDB(db)
	})
}

func TestKhighDB_encodeKey_decodeKey(t *testing.T) {
	db := &KhighDB{}
	key, field := "KHighness", "score"
	buf := db.encodeKey([]byte(key), []byte(field))
	keyBuf, fieldBuf := db.decodeKey(buf)
	assert.Equal(t, key, string(keyBuf))
	assert.Equal(t, field, string(fieldBuf))
}

func newKhighDB(ioType IOType, mode DataIndexMode) *KhighDB {
	path := filepath.Join("/tmp", "KhighDB")
	options := DefaultOptions(path)
	options.IoType = ioType
	options.IndexMode = mode
	db, err := Open(options)
	if err != nil {
		panic(err)
	}
	return db
}

func destroyDB(db *KhighDB) {
	if db != nil {
		if err := db.Close(); err != nil {
			panic(err)
		}
		if runtime.GOOS == "windows" {
			time.Sleep(100 * time.Millisecond)
		}
		if err := os.RemoveAll(db.options.DBPath); err != nil {
			panic(err)
		}
	}
}

func TestGet(t *testing.T) {
	key := getKey(87274365)
	t.Log(len(key), ",", string(key))
	value16B := getValue16B()
	t.Log(len(value16B), ",", string(value16B))
	value4K := getValue4K()
	t.Log(len(value4K), ",", string(value4K))
}

// getKey returns a key whose size is 32 bytes.
func getKey(n int) []byte {
	return []byte("khighdb-store-bench-key" + fmt.Sprintf("%09d", n))
}

// getValue16B return a value whose size is 16 bytes.
func getValue16B() []byte {
	return getValue(1 << 4)
}

// getValue4K returns a value whose size is 4 KB.
func getValue4K() []byte {
	return getValue(4 << 10)
}

// getValue returns a byte slice according to the given n.
func getValue(n int) []byte {
	var str bytes.Buffer
	for i := 0; i < n; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
}

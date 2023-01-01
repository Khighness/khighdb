package khighdb

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-29

func TestOpen(t *testing.T) {
	path := filepath.Join("/tmp", "KhighDB")

	t.Run("default", func(t *testing.T) {
		options := DefaultOptions(path)
		db, err := Open(options)
		assert.Nil(t, err)
		assert.NotNil(t, db)
		defer destroyDB(db)
	})

	t.Run("mmap", func(t *testing.T) {
		options := DefaultOptions(path)
		options.IoType = MMap
		db, err := Open(options)
		assert.Nil(t, err)
		assert.NotNil(t, db)
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

func destroyDB(db *KhighDB)  {
	if db != nil {
		if err := db.Close(); err != nil {
			panic(err)
		}
		if runtime.GOOS == "windows" {
			time.Sleep(100 *time.Millisecond)
		}
		if err := os.RemoveAll(db.options.DBPath); err != nil {
			panic(err)
		}
	}
}

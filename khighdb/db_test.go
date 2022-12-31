package khighdb

import (
	"path/filepath"
	"testing"

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
	})

	// TODO: fixed.
	t.Run("mmap", func(t *testing.T) {
		options := DefaultOptions(path)
		options.IoType = MMap
		db, err := Open(options)
		assert.Nil(t, err)
		assert.NotNil(t, db)
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

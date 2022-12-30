package khighdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-29

func TestKhighDB_encodeKey_decodeKey(t *testing.T) {
	db := &KhighDB{}
	key, field := "KHighness", "score"
	buf := db.encodeKey([]byte(key), []byte(field))
	keyBuf, fieldBuf := db.decodeKey(buf)
	assert.Equal(t, key, string(keyBuf))
	assert.Equal(t, field, string(fieldBuf))
}

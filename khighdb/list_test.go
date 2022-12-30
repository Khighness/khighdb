package khighdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-29

func TestKhighDB_encodeListKey_decodeListKey(t *testing.T) {
	db := &KhighDB{}
	key1, key2 := "K1", "K2"
	listKey1 := db.encodeListKey([]byte(key1), 1)
	listKey2 := db.encodeListKey([]byte(key2), 2)
	decodeKey1, seq1 := db.decodeListKey(listKey1)
	decodeKey2, seq2 := db.decodeListKey(listKey2)
	assert.Equal(t, key1, string(decodeKey1))
	assert.Equal(t, 1, int(seq1))
	assert.Equal(t, key2, string(decodeKey2))
	assert.Equal(t, 2, int(seq2))
}

package store

import (
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-11-16

var (
	key   = []byte("K1")
	val   = []byte("K2")
	extra = []byte("extra data")
)

func TestNewEntry(t *testing.T) {
	t.Logf("%+v", NewEntry(key, val, extra, String, 0))
}

func TestNewEntryWithExpire(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		deadline := time.Now().Add(21 * time.Second).Unix()
		e := NewEntryWithExpire(key, val, deadline, String, 2)
		assert.NotEqual(t, e, nil)
	})

	t.Run("val nil", func(t *testing.T) {
		deadline := time.Now().Add(21 * time.Second).Unix()
		e := NewEntryWithExpire(key, nil, deadline, String, 2)
		assert.NotEqual(t, e, nil)
	})
}

func TestNewEntryWithoutExtra(t *testing.T) {
	e := NewEntryWithoutExtra([]byte("key001"), []byte("val001"), 1, 2)
	assert.NotEqual(t, e, nil)
}

func TestEntry_GetType(t *testing.T) {
	deadline := time.Now().Add(21 * time.Second).Unix()
	e := NewEntryWithExpire(key, val, deadline, ZSet, 15)
	assert.Equal(t, e.GetType(), uint16(ZSet))
}

func TestEntry_GetMark(t *testing.T) {
	deadline := time.Now().Add(21 * time.Second).Unix()
	e := NewEntryWithExpire(key, val, deadline, ZSet, 15)
	assert.Equal(t, e.GetMark(), uint16(15))
}

func TestEntry_Encode(t *testing.T) {
	file := strings.ReplaceAll("testdata/entry/test.db", "/", string(os.PathSeparator))

	t.Run("test1", func(t *testing.T) {
		e := &Entry{Meta: &Meta{Key: key, Value: val}}
		e.Meta.KeySize = uint32(len(e.Meta.Key))
		e.Meta.ValueSize = uint32(len(e.Meta.Value))

		encVal, err := Encode(e)
		if err != nil {
			t.Error(err)
		}
		t.Log(e.Size())
		t.Log(encVal)

		if encVal != nil {
			file, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Error(err)
			}
			file.Write(encVal)
		}
	})

	t.Run("test2", func(t *testing.T) {
		e := &Entry{Meta: &Meta{Key: key, Value: val}}
		e.Meta.KeySize = uint32(len(e.Meta.Key))
		e.Meta.ValueSize = uint32(len(e.Meta.Value))

		encVal, err := Encode(e)
		if err != nil {
			t.Error(err)
		}
		t.Log(e.Size())
		t.Log(encVal)
	})
}

func TestDecode(t *testing.T) {
	file := strings.ReplaceAll("testdata/entry/test.db", "/", string(os.PathSeparator))
	// expected val : [169 64 25 4 0 0 0 13 0 0 0 15 116 101 115 116 95 107 101 121 95 48 48 48 49 116 101 115 116 95 118 97 108 117 101 95 48 48 48 49]
	if file, err := os.OpenFile(file, os.O_RDONLY, os.ModePerm); err != nil {
		t.Error("open File err ", err)
	} else {
		buf := make([]byte, entryHeaderSize)
		var offset int64 = 0
		if n, err := file.ReadAt(buf, offset); err != nil {
			t.Error("read data err ", err)
		} else {
			t.Log("success read ", n)
			t.Log(buf)
			e, _ := Decode(buf)

			// read key
			offset += entryHeaderSize
			if e.Meta.KeySize > 0 {
				key := make([]byte, e.Meta.KeySize)
				file.ReadAt(key, offset)
				e.Meta.Key = key
			}

			// read value
			offset += int64(e.Meta.KeySize)
			if e.Meta.ValueSize > 0 {
				val := make([]byte, e.Meta.ValueSize)
				file.ReadAt(val, offset)
				e.Meta.Value = val
			}

			t.Logf("Key = %s, Value = %s, KeySize = %d, ValueSize = %d\n",
				string(e.Meta.Key), string(e.Meta.Value), e.Meta.KeySize, e.Meta.ValueSize)

			checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
			t.Log(checkCrc, e.crc32)
		}
	}
}

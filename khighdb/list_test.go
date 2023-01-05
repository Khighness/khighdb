package khighdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-29

func TestKhighDB_LPush(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPush(t, FileIO, KeyOnlyMemMode, true)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPush(t, MMap, KeyOnlyMemMode, true)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBPush(t, FileIO, KeyValueMemMode, true)
	})
}

func TestKhighDB_LPushX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPushX(t, FileIO, KeyOnlyMemMode, true)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPushX(t, MMap, KeyOnlyMemMode, true)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBPushX(t, FileIO, KeyValueMemMode, true)
	})
}

func TestKhighDB_RPush(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPush(t, FileIO, KeyOnlyMemMode, false)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPush(t, MMap, KeyOnlyMemMode, false)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBPush(t, FileIO, KeyValueMemMode, false)
	})
}

func TestKhighDB_RPushX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPushX(t, FileIO, KeyOnlyMemMode, false)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPushX(t, MMap, KeyOnlyMemMode, false)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBPushX(t, FileIO, KeyValueMemMode, false)
	})
}

func TestKhighDB_LIndex(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBLIndex(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBLIndex(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBLIndex(t, FileIO, KeyValueMemMode)
	})
}

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

func testKhighDBPush(t *testing.T, ioType IOType, mode DataIndexMode, isLeft bool) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil, values: nil}, false,
		},
		{
			"nil-val", db, args{key: getKey(1), values: nil}, false,
		},
		{
			"one-val", db, args{key: getKey(2), values: [][]byte{getValue16B()}}, false,
		},
		{
			"more-val", db, args{key: getKey(2), values: [][]byte{getValue16B(), getValue16B()}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isLeft {
				if err := tt.db.LPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPush() error = %v, wantErr = %v", err, tt.wantErr)
				}
			} else {
				if err := tt.db.RPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("RPush() error = %v, wantErr = %v", err, tt.wantErr)
				}
			}
		})
	}
}

func testKhighDBPushX(t *testing.T, ioType IOType, mode DataIndexMode, isLeft bool) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.LPush(getKey(0), nil)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		wantErr bool
		err     error
	}{
		{
			"nil-key", db, args{key: nil, values: nil}, true, ErrKeyNotFound,
		},
		{
			"nil-val", db, args{key: getKey(0), values: nil}, false, nil,
		},
		{
			"one-val", db, args{key: getKey(0), values: [][]byte{getValue16B()}}, false, nil,
		},
		{
			"more-val", db, args{key: getKey(0), values: [][]byte{getValue16B(), getValue16B()}}, false, nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isLeft {
				if err := tt.db.LPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("LPushX() error = %v, wantErr = %v", err, tt.wantErr)
				}
			} else {
				if err := tt.db.RPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
					t.Errorf("RPushX() error = %v, wantErr = %v", err, tt.wantErr)
				}
			}
		})
	}
}

func testKhighDBLIndex(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	// none
	listKey := getKey(0)
	v, err := db.LIndex(listKey, 0)
	assert.Nil(t, err)
	assert.Nil(t, v)

	// left
	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		_ = db.LPush(listKey, val)
		got, err := db.LIndex(listKey, 0)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}

	// 10 9 8 7 6 5 4 3 2 1
	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		got, err := db.LIndex(listKey, 10-i)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
		got, err = db.LIndex(listKey, -i)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}

	_, err = db.LIndex(listKey, 10)
	assert.Equal(t, ErrIndexOutOfRange, err)
	_, err = db.LIndex(listKey, -11)
	assert.Equal(t, ErrIndexOutOfRange, err)

	// right
	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		_ = db.RPush(listKey, val)
		got, err := db.LIndex(listKey, -1)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}

	// 10 9 8 7 6 5 4 3 2 1 1 2 3 4 5 6 7 8 9 10
	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		got, err := db.LIndex(listKey, 9+i)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
		got, err = db.LIndex(listKey, i-11)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}

	_, err = db.LIndex(listKey, 20)
	assert.Equal(t, ErrIndexOutOfRange, err)
	_, err = db.LIndex(listKey, -21)
	assert.Equal(t, ErrIndexOutOfRange, err)
}

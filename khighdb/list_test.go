package khighdb

import (
	"errors"
	"fmt"
	"reflect"
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
}

func TestKhighDB_LPushX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPushX(t, FileIO, KeyOnlyMemMode, true)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPushX(t, MMap, KeyOnlyMemMode, true)
	})
}

func TestKhighDB_RPush(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPush(t, FileIO, KeyOnlyMemMode, false)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPush(t, MMap, KeyOnlyMemMode, false)
	})
}

func TestKhighDB_RPushX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPushX(t, FileIO, KeyOnlyMemMode, false)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPushX(t, MMap, KeyOnlyMemMode, false)
	})
}

func TestKhighDB_LPop(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBLPop(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBLPop(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_RPop(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBRPop(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBRPop(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_LMove(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBLMove(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBLMove(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_LLen(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBRLLen(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBRLLen(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_LIndex(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBLIndex(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBLIndex(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_LRange(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBLRange(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBLRange(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_LRem(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBLRem(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBLRem(t, MMap, KeyOnlyMemMode)
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

func testKhighDBLPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	listKey := getKey(0)

	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		_ = db.LPush(listKey, val)
	}
	for i := 10; i >= 1; i-- {
		val := []byte(fmt.Sprintf("v-%d", i))
		got, err := db.LPop(listKey)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}

	got, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.Nil(t, got)
}

func testKhighDBRPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	listKey := getKey(0)

	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		_ = db.RPush(listKey, val)
	}
	for i := 10; i >= 1; i-- {
		val := []byte(fmt.Sprintf("v-%d", i))
		got, err := db.RPop(listKey)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}

	got, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.Nil(t, got)
}

func testKhighDBLMove(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	listKey1 := getKey(1)
	listKey2 := getKey(2)

	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		_ = db.RPush(listKey1, val)
	}
	for i := 1; i <= 10; i++ {
		val := []byte(fmt.Sprintf("v-%d", i))
		rem, err := db.LMove(listKey1, listKey2, true, true)
		assert.Nil(t, err)
		assert.Equal(t, val, rem)
	}
	for i := 10; i >= 1; i-- {
		val := []byte(fmt.Sprintf("v-%d", i))
		got, err := db.LPop(listKey2)
		assert.Nil(t, err)
		assert.Equal(t, val, got)
	}
}

func testKhighDBRLLen(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	listKey := getKey(0)

	for i := 1; i <= 10; i++ {
		_ = db.RPush(listKey, getValue16B())
		assert.Equal(t, i, db.LLen(listKey))
	}
	for i := 1; i <= 10; i++ {
		_, _ = db.RPop(listKey)
		assert.Equal(t, 10-i, db.LLen(listKey))
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

	// list: 10 9 8 7 6 5 4 3 2 1
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

	// list: 10 9 8 7 6 5 4 3 2 1 1 2 3 4 5 6 7 8 9 10
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

func testKhighDBLRange(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	listKey := getKey(0)
	_, err := db.LRange(listKey, 0, 0)
	assert.Equal(t, ErrKeyNotFound, err)

	for i := 1; i <= 10; i++ {
		_ = db.RPush(listKey, []byte(fmt.Sprintf("v-%d", i)))
	}

	type args struct {
		start int
		end   int
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		want    [][]byte
		err     error
		wantErr bool
	}{
		{
			"want[0, 0]", db, args{0, 0}, [][]byte{[]byte("v-1")}, nil, false,
		},
		{
			"want[8, 9]", db, args{8, 9}, [][]byte{[]byte("v-9"), []byte("v-10")}, nil, false,
		},
		{
			"want[8, 11]", db, args{8, 11}, [][]byte{[]byte("v-9"), []byte("v-10")}, nil, false,
		},
		{
			"want[5, 4]", db, args{5, 3}, [][]byte{}, ErrIndexOutOfRange, true,
		},
		{
			"want[-2, -1]", db, args{-2, -1}, [][]byte{[]byte("v-9"), []byte("v-10")}, nil, false,
		},
		{
			"want[-13, -10]", db, args{-13, -10}, [][]byte{[]byte("v-1")}, nil, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.LRange(listKey, tt.args.start, tt.args.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("LRange() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !errors.Is(err, tt.err) {
				t.Errorf("LRange() error = %v, excepted err = %v", err, tt.err)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LRange() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBLRem(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	listKey := []byte("my_list")
	v, err := db.LRem(listKey, 1, getKey(1))
	assert.Equal(t, 0, v)
	assert.Nil(t, err)
	v, err = db.LRem(listKey, 0, getKey(1))
	assert.Equal(t, 0, v)
	assert.Nil(t, err)
	v, err = db.LRem(listKey, -1, getKey(1))
	assert.Equal(t, 0, v)
	assert.Nil(t, err)

	err = db.RPush(listKey, getKey(1), getKey(2), getKey(1), getKey(3), getKey(3), getKey(4))
	assert.Nil(t, err)

	// list : 1 2 1 3 3 4
	expected := [][]byte{getKey(1), getKey(2), getKey(1), getKey(3), getKey(3), getKey(4)}
	v, err = db.LRem(listKey, 1, getKey(5))
	assert.Equal(t, 0, v)
	assert.Nil(t, err)
	values, err := db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 1 2 1 3 3 4
	expected = [][]byte{getKey(1), getKey(2), getKey(1), getKey(3), getKey(3), getKey(4)}
	v, err = db.LRem(listKey, 0, getKey(5))
	assert.Equal(t, 0, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 1 2 1 3 3 4
	expected = [][]byte{getKey(1), getKey(2), getKey(1), getKey(3), getKey(3), getKey(4)}
	v, err = db.LRem(listKey, -1, getKey(5))
	assert.Equal(t, 0, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 1 2 1 3 3 4
	expected = [][]byte{getKey(2), getKey(3), getKey(3), getKey(4)}
	v, err = db.LRem(listKey, 3, getKey(1))
	assert.Equal(t, 2, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 2 3 3 4
	expected = [][]byte{getKey(2), getKey(4)}
	v, err = db.LRem(listKey, -3, getKey(3))
	assert.Equal(t, 2, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 2 4
	expected = [][]byte{getKey(4)}
	v, err = db.LRem(listKey, 0, getKey(2))
	assert.Equal(t, 1, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 4
	err = db.RPush(listKey, getKey(3), getKey(2), getKey(1))
	assert.Nil(t, err)

	// list : 4 3 2 1
	expected = [][]byte{getKey(3), getKey(2), getKey(1)}
	v, err = db.LRem(listKey, 1, getKey(4))
	assert.Equal(t, 1, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 3 2 1
	expected = [][]byte{getKey(3), getKey(2)}
	v, err = db.LRem(listKey, -1, getKey(1))
	assert.Equal(t, 1, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)

	// list : 3 2
	expected = [][]byte{getKey(3)}
	v, err = db.LRem(listKey, 0, getKey(2))
	assert.Equal(t, 1, v)
	assert.Nil(t, err)
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, expected, values)
	assert.Nil(t, err)
}

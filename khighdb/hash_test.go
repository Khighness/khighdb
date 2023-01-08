package khighdb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2023-01-08

func TestKhighDB_HSet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHSet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHSet(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_HSetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHSetNX(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHSetNX(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_HGet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHGet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHGet(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_HMGet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHMGet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHMGet(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_HDel(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHDel(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHDel(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_HExists(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHExists(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHExists(t, MMap, KeyOnlyMemMode)
	})
}

func TestKhighDB_HLe(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBHLen(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBHLen(t, MMap, KeyOnlyMemMode)
	})
}

func testKhighDBHSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	type args struct {
		key    []byte
		fields [][]byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		wantErr bool
	}{
		{
			"nil-key-nil-field", db, args{nil, [][]byte{}}, true,
		},
		{
			"nil-key", db, args{nil, [][]byte{nil, []byte("v-1")}}, false,
		},
		{
			"normal-single-pair", db, args{[]byte("k-1"), [][]byte{[]byte("f-1"), []byte("v-1")}}, false,
		},
		{
			"normal-multiple-pair", db, args{[]byte("k-2"), [][]byte{[]byte("f-1"), []byte("v-1"), []byte("f-2"), []byte("v-2")}}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.HSet(tt.args.key, tt.args.fields...)
			if (err != nil) != tt.wantErr {
				t.Errorf("HSet() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrInvalidNumberOfArgs) {
				t.Errorf("HSet() error = %v, expected error = %v", err, ErrInvalidNumberOfArgs)
			}
		})
	}
}

func testKhighDBHSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.HSet([]byte("k-1"), []byte("f-1"), []byte("v-1"))
	_ = db.HSet([]byte("k-1"), []byte("f-2"), []byte("v-2"))

	type args struct {
		key   []byte
		field []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		wantRes bool
		wantErr bool
	}{
		{
			"exist-key", db, args{[]byte("k-1"), []byte("f-0"), []byte("v-0")}, true, false,
		},
		{
			"missing-key", db, args{[]byte("k-2"), []byte("f-0"), []byte("v-0")}, true, false,
		},
		{
			"exist-field", db, args{[]byte("k-1"), []byte("f-1"), []byte("v-1")}, false, false,
		},
		{
			"missing-field", db, args{[]byte("k-1"), []byte("f-3"), []byte("v-3")}, true, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.db.HSetNX(tt.args.key, tt.args.field, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("HSetNX() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if result != tt.wantRes {
				t.Errorf("HSetNX() result = %v, wantRes = %v", result, tt.wantRes)
			}
		})
	}
}

func testKhighDBHGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	type args struct {
		key   []byte
		field []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		wantErr bool
	}{
		{
			"nil-key-nil-field", db, args{nil, nil, []byte("v-0")}, false,
		},
		{
			"normal", db, args{[]byte("k-1"), []byte("f-1"), []byte("v-1")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = tt.db.HSet(tt.args.key, tt.args.field, tt.args.value)
			got, err := tt.db.HGet(tt.args.key, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("HGet() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !bytes.Equal(got, tt.args.value) {
				t.Errorf("HGet() got = %v, want = %v", got, tt.args.value)
			}
		})
	}
}

func testKhighDBHMGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	hashKey := []byte("hash-key")
	_ = db.HSet(hashKey, []byte("f-1"), []byte("v-1"))
	_ = db.HSet(hashKey, []byte("f-2"), []byte("v-2"))
	_ = db.HSet(hashKey, []byte("f-3"), []byte("v-3"))

	type args struct {
		key   []byte
		field [][]byte
	}

	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			"missing-key", db, args{nil, [][]byte{}}, nil, false,
		},
		{
			"missing-field", db, args{hashKey, [][]byte{[]byte("f-0")}}, [][]byte{nil}, false,
		},
		{
			"normal-1", db, args{hashKey, [][]byte{[]byte("f-1")}}, [][]byte{[]byte("v-1")}, false,
		},
		{
			"normal-2", db, args{hashKey, [][]byte{[]byte("f-2"), []byte("f-3")}}, [][]byte{[]byte("v-2"), []byte("v-3")}, false,
		},
		{
			"duplicate-filed", db, args{hashKey, [][]byte{[]byte("f-3"), []byte("f-3")}}, [][]byte{[]byte("v-3"), []byte("v-3")}, false,
		},
		{
			"multiple-mssing-filed", db, args{hashKey, [][]byte{[]byte("f-3"), []byte("f-4")}}, [][]byte{[]byte("v-3"), nil}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HMGet(tt.args.key, tt.args.field...)
			if (err != nil) != tt.wantErr {
				t.Errorf("HMGet() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HMGet() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBHDel(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	hashKey := []byte("hash-key")
	del, err := db.HDel(hashKey, nil)
	assert.Nil(t, err)
	assert.Equal(t, 0, del)

	fields := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		field := []byte(fmt.Sprintf("f-%d", i))
		value := []byte(fmt.Sprintf("v-%d", i))
		fields[i] = field
		values[i] = value
		_ = db.HSet(hashKey, field, value)
	}

	got, err := db.HMGet(hashKey, fields...)
	assert.Nil(t, err)
	assert.Equal(t, values, got)

	del, err = db.HDel(hashKey, fields...)
	assert.Nil(t, err)
	assert.Equal(t, 10, del)

	for i := 0; i < 10; i++ {
		values[i] = nil
	}
	got, err = db.HMGet(hashKey, fields...)
	assert.Nil(t, err)
	assert.Equal(t, values, got)

	del, err = db.HDel(hashKey, fields...)
	assert.Nil(t, err)
	assert.Equal(t, 0, del)
}

func testKhighDBHExists(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	hashKey := []byte("hash-key")
	_ = db.HSet(hashKey, getKey(1), getValue16B())

	got, err := db.HMGet(hashKey, getKey(1))
	assert.Nil(t, err)
	t.Log(got)

	exist, err := db.HExists(hashKey, getKey(1))
	assert.Nil(t, err)
	assert.Equal(t, true, exist)

	exist, err = db.HExists(hashKey, getKey(2))
	assert.Nil(t, err)
	assert.Equal(t, false, exist)
}

func testKhighDBHLen(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	hashKey := []byte("hash-key")

	l := db.HLen(hashKey)
	assert.Equal(t, 0, l)

	for i := 1; i <= 10; i++ {
		_ = db.HSet(hashKey, getKey(i), getValue16B())
		l = db.HLen(hashKey)
		assert.Equal(t, i, l)
	}

	for i := 1; i <= 10; i++ {
		_, _ = db.HDel(hashKey, getKey(i))
		l = db.HLen(hashKey)
		assert.Equal(t, 10-i, l)
	}
}

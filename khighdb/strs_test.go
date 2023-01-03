package khighdb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2023-01-03

func TestKhighDB_Set(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBSet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBSet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBSet(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Set_LogFileThreshold(t *testing.T) {
	path := filepath.Join("/tmp", "KhighDB")
	opts := DefaultOptions(path)
	opts.IoType = MMap
	// 32 MB
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 600000; i++ {
		err := db.Set(getKey(i), getValue16B())
		assert.Nil(t, err)
	}
}

func TestKhighDB_Get(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBGet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBGet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBGet(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_MSet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBMSet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBMSet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBMSet(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_MGet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBMGet(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBMGet(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBMGet(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_GetRange(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBGetRange(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBGetRange(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBGetRange(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_GetDel(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBGetDel(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBGetDel(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBGetDel(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Delete(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBDelete(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBDelete(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBDelete(t, FileIO, KeyValueMemMode)
	})
}

func testKhighDBSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil, value: []byte("val-1")}, false,
		},
		{
			"nil-value", db, args{key: []byte("key-1"), value: nil}, false,
		},
		{
			"normal", db, args{key: []byte("key-2"), value: []byte("val-2")}, false,
		},
		{
			"overwritten", db, args{key: []byte("key-2"), value: []byte("val-3")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.Set(tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func testKhighDBGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set(nil, []byte("zero"))
	_ = db.Set([]byte("zero"), nil)
	for i := 1; i <= 3; i++ {
		_ = db.Set([]byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i)))
	}
	_ = db.Set([]byte("k-3"), []byte("v-33"))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil}, []byte("zero"), false,
		},
		{
			"nil-val", db, args{key: []byte("nil")}, nil, true,
		},
		{
			"normal", db, args{key: []byte("k-1")}, []byte("v-1"), false,
		},
		{
			"rewrite", db, args{key: []byte("k-3")}, []byte("v-33"), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBMSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	tests := []struct {
		name    string
		db      *KhighDB
		args    [][]byte
		wantErr bool
	}{
		{
			"nil-key", db, [][]byte{nil, []byte("zero")}, false,
		},
		{
			"nil-val", db, [][]byte{[]byte("zero"), nil}, false,
		},
		{
			"empty-pair", db, [][]byte{nil, nil}, false,
		},
		{
			"one-pair", db, [][]byte{[]byte("k-1"), []byte("v-1")}, false,
		},
		{
			"multiple-pair", db, [][]byte{[]byte("k-1"), []byte("v-1"), []byte("k-2"), []byte("v-2"), []byte("k-3"), []byte("v-3")}, false,
		},
		{
			"invalid-pair", db, [][]byte{[]byte("k-1"), []byte("v-1"), []byte("k-2"), []byte("v-2"), []byte("k-3")}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.MSet(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSet() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == true && !errors.Is(err, ErrInvalidNumberOfArgs) {
				t.Errorf("MSet() error = %v, expected error = %v", err, ErrInvalidValueType)
			}
		})
	}
}

func testKhighDBMGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set(nil, []byte("zero"))
	_ = db.Set([]byte("zero"), nil)
	for i := 1; i <= 3; i++ {
		_ = db.Set([]byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i)))
	}

	tests := []struct {
		name    string
		db      *KhighDB
		args    [][]byte
		want    [][]byte
		wantErr bool
	}{
		{
			"nil-key", db, [][]byte{nil}, [][]byte{[]byte("zero")}, false,
		},
		{
			"nil-val", db, [][]byte{[]byte("zero")}, [][]byte{}, false,
		},
		{
			"single-key", db, [][]byte{[]byte("k-1")}, [][]byte{[]byte("v-1")}, false,
		},
		{
			"nil-key-in-multiple-key", db, [][]byte{nil, []byte("k-1"), []byte("k-2"), []byte("k-3")}, [][]byte{[]byte("zero"), []byte("v-1"), []byte("v-2"), []byte("v-3")}, false,
		},
		{
			"multiple-key", db, [][]byte{[]byte("k-1"), []byte("k-2"), []byte("k-3")}, [][]byte{[]byte("v-1"), []byte("v-2"), []byte("v-3")}, false,
		},
		{
			"missed-one-key", db, [][]byte{[]byte("k-1"), []byte("k-22"), []byte("k-3")}, [][]byte{[]byte("v-1"), nil, []byte("v-3")}, false,
		},
		{
			"missed-two-key", db, [][]byte{[]byte("k-1"), []byte("k-22"), []byte("k-33")}, [][]byte{[]byte("v-1"), nil, nil}, false,
		},
		{
			"missed-two-key", db, [][]byte{[]byte("k-1"), []byte("k-22"), []byte("k-33")}, [][]byte{[]byte("v-1"), nil, nil}, false,
		},
		{
			"missed-all-key", db, [][]byte{[]byte("k-11"), []byte("k-22"), []byte("k-33")}, [][]byte{nil, nil, nil}, false,
		},
		{
			"empty-key", db, [][]byte{}, nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.MGet(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("MGet() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if len(got) != 0 && len(tt.want) != 0 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MGet() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBGetRange(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	key := []byte("key")
	val := []byte("test-val")
	_ = db.Set(key, val)
	keyEmpty := []byte("key-empty")
	valEmpty := []byte("")
	_ = db.Set(keyEmpty, valEmpty)

	type args struct {
		key   []byte
		start int
		end   int
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"key-not-found", args{[]byte("missing-key"), 0, 7}, nil, true,
		},
		{
			"key-empty", args{keyEmpty, 0, 7}, nil, false,
		},
		{
			"val[0, 7]", args{keyEmpty, 0, 7}, val, false,
		},
		{
			"val[1, 6]", args{keyEmpty, 1, 6}, val[1:6], false,
		},
		{
			"val[0, 8]", args{keyEmpty, 0, 8}, val, false,
		},
		{
			"val[5, 3]", args{keyEmpty, 5, 3}, []byte{}, false,
		},
		{
			"val[0, -1]", args{keyEmpty, 0, -1}, val, false,
		},
		{
			"val[-1, 7]", args{keyEmpty, -1, 7}, val[7:], false,
		},
		{
			"val[-9, 0]", args{keyEmpty, -9, 0}, val[0:1], false,
		},
		{
			"val[7, -1]", args{keyEmpty, 7, -1}, val[7:], false,
		},
		{
			"val[0, -9]", args{keyEmpty, 0, -9}, val[0:1], false,
		},
		{
			"val[-5, -3]", args{keyEmpty, -5, 3}, val[3:5], false,
		},
		{
			"val[-3, -5]", args{keyEmpty, -1, 7}, []byte{}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.GetRange(tt.args.key, tt.args.start, tt.args.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRange() error = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if len(got) != 0 && len(tt.want) != 0 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRange() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBGetDel(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	var kvPairs [][]byte
	for i := 1; i < 3; i++ {
		kvPairs = append(kvPairs, []byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i)))
	}
	_ = db.MSet(kvPairs...)

	tests := []struct {
		name   string
		db     *KhighDB
		key    []byte
		expVal []byte
		expErr error
	}{
		{
			"k-1-exist", db, []byte("k-1"), []byte("v-1"), nil,
		},
		{
			"k-1-not-exist", db, []byte("k-1"), nil, nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := tt.db.GetDel(tt.key)
			if err != tt.expErr {
				t.Errorf("GetDel(): expected error = %+v, actual error = %+v", tt.expErr, err)
			}
			if !bytes.Equal(val, tt.expVal) {
				t.Errorf("GetDel(): expected val = %v, actual val = %v", tt.expVal, val)
			}

			val, _ = tt.db.Get(tt.key)
			if val != nil {
				t.Errorf("GetDel(): expected val(after Get()): <nil>, actual val(after Get()): %v", val)
			}
		})
	}
}

func testKhighDBDelete(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	var kvPairs [][]byte
	for i := 1; i < 3; i++ {
		kvPairs = append(kvPairs, []byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i)))
	}
	_ = db.MSet(kvPairs...)

	tests := []struct {
		name    string
		db      *KhighDB
		args    []byte
		want    []byte
		wantErr bool
	}{
		{
			"get-before-delete", db, []byte("k-1"), []byte("v-1"), false,
		},
		{
			"get-after-delete", db, []byte("k-1"), nil, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Get(tt.args)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("Delete() got = %v, want = %v", got, tt.want)
			}
			err = tt.db.Delete(tt.args)
			assert.Nil(t, err)
		})
	}
}

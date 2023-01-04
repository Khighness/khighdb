package khighdb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

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

func TestKhighDB_SetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBSetNX(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBSetNX(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBSetNX(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_SetEX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBSetEX(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBSetEX(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBSetEX(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_MSetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBMSetNX(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBMSetNX(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBMSetNX(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Append(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBMAppend(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBMAppend(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBMAppend(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Incr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBIncr(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBIncr(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBIncr(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Decr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBDecr(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBDecr(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBDecr(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_IncrBy(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBIncrBy(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBIncrBy(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBIncrBy(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_DecrBy(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBDecrBy(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBDecrBy(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBDecrBy(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_StrLen(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBStrLen(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBStrLen(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBStrLen(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Count(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBCount(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBCount(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBCount(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Scan(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBScan(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBScan(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBScan(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Expire(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBExpire(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBExpire(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBExpire(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_TTL(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBTTL(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBTTL(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBTTL(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_Persist(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBPersist(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBPersist(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBPersist(t, FileIO, KeyValueMemMode)
	})
}

func TestKhighDB_GetStrKeys(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testKhighDBGetStrKeys(t, FileIO, KeyOnlyMemMode)
	})

	t.Run("mmap", func(t *testing.T) {
		testKhighDBGetStrKeys(t, MMap, KeyOnlyMemMode)
	})

	t.Run("key-val-mem-mode", func(t *testing.T) {
		testKhighDBGetStrKeys(t, FileIO, KeyValueMemMode)
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
			"nil-key", db, args{key: nil, value: []byte("want-1")}, false,
		},
		{
			"nil-value", db, args{key: []byte("key-1"), value: nil}, false,
		},
		{
			"normal", db, args{key: []byte("key-2"), value: []byte("want-2")}, false,
		},
		{
			"overwritten", db, args{key: []byte("key-2"), value: []byte("want-3")}, false,
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
			"nil-want", db, args{key: []byte("nil")}, nil, true,
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
			"nil-want", db, [][]byte{[]byte("zero"), nil}, false,
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
			"nil-want", db, [][]byte{[]byte("zero")}, [][]byte{}, false,
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
	val := []byte("test-want")
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
			"want[0, 7]", args{keyEmpty, 0, 7}, val, false,
		},
		{
			"want[1, 6]", args{keyEmpty, 1, 6}, val[1:6], false,
		},
		{
			"want[0, 8]", args{keyEmpty, 0, 8}, val, false,
		},
		{
			"want[5, 3]", args{keyEmpty, 5, 3}, []byte{}, false,
		},
		{
			"want[0, -1]", args{keyEmpty, 0, -1}, val, false,
		},
		{
			"want[-1, 7]", args{keyEmpty, -1, 7}, val[7:], false,
		},
		{
			"want[-9, 0]", args{keyEmpty, -9, 0}, val[0:1], false,
		},
		{
			"want[7, -1]", args{keyEmpty, 7, -1}, val[7:], false,
		},
		{
			"want[0, -9]", args{keyEmpty, 0, -9}, val[0:1], false,
		},
		{
			"want[-5, -3]", args{keyEmpty, -5, 3}, val[3:5], false,
		},
		{
			"want[-3, -5]", args{keyEmpty, -1, 7}, []byte{}, false,
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
		name    string
		db      *KhighDB
		key     []byte
		want    []byte
		wantErr bool
	}{
		{
			"k-1-exist", db, []byte("k-1"), []byte("v-1"), false,
		},
		{
			"k-1-not-exist", db, []byte("k-1"), nil, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.GetDel(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetDel() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("GetDel() got = %v, want = %v", got, tt.want)
			}
			got, _ = tt.db.Get(tt.key)
			if got != nil {
				t.Errorf("GetDel() got = %v, want = nil ", got)
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

func testKhighDBSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    []args
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", db, []args{args{key: nil, value: []byte("zero")}}, []byte("zero"), false,
		},
		{
			"nil-key-rewrite", db, []args{args{key: nil, value: []byte("0")}}, []byte("zero"), false,
		},
		{
			"normal-key", db, []args{args{key: []byte("k-1"), value: []byte("v-1")}}, []byte("v-1"), false,
		},
		{
			"normal-rewrite", db, []args{args{key: []byte("k-1"), value: []byte("v-11")}}, []byte("v-1"), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, arg := range tt.args {
				if err := tt.db.SetNX(arg.key, arg.value); (err != nil) != tt.wantErr {
					t.Errorf("SetNX() error = %v, wantErr = %v", err, tt.wantErr)
				}
				got, err := tt.db.Get(arg.key)
				assert.Nil(t, err)
				if !bytes.Equal(got, tt.want) {
					t.Errorf("SetNX() got = %v, want = %v", got, tt.want)
				}
			}
		})
	}
}

func testKhighDBSetEX(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	key, val := []byte("k-ttl"), getValue16B()
	newVal := getValue16B()

	err := db.SetEX(key, val, 1000*time.Millisecond)
	assert.Nil(t, err)

	time.Sleep(300 * time.Millisecond)
	got, err := db.Get(key)
	assert.Nil(t, err)
	if !bytes.Equal(got, val) {
		t.Errorf("SetEX() got = %v, want = %v", got, val)
	}

	time.Sleep(700 * time.Millisecond)
	_, err = db.Get(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("SetEX() error = %v, want = %v", err, ErrKeyNotFound)
	}

	err = db.SetEX(key, newVal, 200*time.Millisecond)
	assert.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
	got, err = db.Get(key)
	assert.Nil(t, err)
	if !bytes.Equal(got, newVal) {
		t.Errorf("SetEX() got = %v, want = %v", got, val)
	}
}

func testKhighDBMSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	tests := []struct {
		name    string
		db      *KhighDB
		args    [][]byte
		key     []byte
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", db, [][]byte{nil, []byte("zero")}, nil, []byte("zero"), false,
		},
		{
			"normal-key", db, [][]byte{[]byte("k-1"), []byte("v-1")}, []byte("k-1"), []byte("v-1"), false,
		},
		{
			"multiple-keys", db, [][]byte{[]byte("k-2"), []byte("v-2"), []byte("k-3"), []byte("v-3")}, []byte("k-3"), []byte("v-3"), false,
		},
		{
			"multiple-keys-rewrite", db, [][]byte{[]byte("k-2"), []byte("v-22"), []byte("k-33"), []byte("v-33")}, []byte("k-3"), []byte("v-3"), false,
		},
		{
			"invalid-args", db, [][]byte{nil, []byte("k-2"), []byte("v-22")}, []byte("k-3"), []byte("v-3"), true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.MSetNX(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSetNX() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr == true && !errors.Is(err, ErrInvalidNumberOfArgs) {
				t.Errorf("MSetNX() error = %v, expected error = %v", err, ErrInvalidNumberOfArgs)
			}
			got, err := tt.db.Get(tt.key)
			assert.Nil(t, err)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("MSetNX() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBMAppend(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	type args struct {
		key []byte
		val []byte
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		key     []byte
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", db, args{nil, []byte("zero")}, nil, []byte("zero"), false,
		},
		{
			"nil-key-append", db, args{nil, []byte("-zero")}, nil, []byte("zero-zero"), false,
		},
		{
			"normal-key", db, args{[]byte("k-1"), []byte("v-1")}, []byte("k-1"), []byte("v-1"), false,
		},
		{
			"normal-key-append", db, args{[]byte("k-1"), []byte("-v-1")}, []byte("k-1"), []byte("v-1-v-1"), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.Append(tt.args.key, tt.args.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("Append() error = %v, wantErr = %v", err, tt.wantErr)
			}
			got, err := tt.db.Get(tt.key)
			assert.Nil(t, err)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("Append() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBIncr(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set([]byte("k-str"), []byte("v-str"))
	_ = db.Set([]byte("k-max"), []byte(strconv.Itoa(math.MaxInt64)))

	tests := []struct {
		name    string
		db      *KhighDB
		key     []byte
		want    []byte
		wantErr bool
		err     error
	}{
		{
			"nil-key-init", db, nil, []byte("1"), false, nil,
		},
		{
			"nil-key-incr", db, nil, []byte("2"), false, nil,
		},
		{
			"normal-key-init", db, []byte("k-1"), []byte("1"), false, nil,
		},
		{
			"normal-key-incr-first", db, []byte("k-1"), []byte("2"), false, nil,
		},
		{
			"normal-key-incr-second", db, []byte("k-1"), []byte("3"), false, nil,
		},
		{
			"int-overflow", db, []byte("k-max"), []byte("0"), true, ErrIntegerOverflow,
		},
		{
			"invalid-type", db, []byte("k-str"), []byte("0"), true, ErrInvalidValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Incr(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Incr() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !bytes.Equal([]byte(strconv.Itoa(int(got))), tt.want) {
				t.Errorf("Incr() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBDecr(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set([]byte("k-str"), []byte("v-str"))
	_ = db.Set([]byte("k-min"), []byte(strconv.Itoa(math.MinInt64)))

	tests := []struct {
		name    string
		db      *KhighDB
		key     []byte
		want    []byte
		wantErr bool
		err     error
	}{
		{
			"nil-key-init", db, nil, []byte("-1"), false, nil,
		},
		{
			"nil-key-incr", db, nil, []byte("-2"), false, nil,
		},
		{
			"normal-key-init", db, []byte("k-1"), []byte("-1"), false, nil,
		},
		{
			"normal-key-decr-first", db, []byte("k-1"), []byte("-2"), false, nil,
		},
		{
			"normal-key-decr-second", db, []byte("k-1"), []byte("-3"), false, nil,
		},
		{
			"int-overflow", db, []byte("k-min"), []byte("0"), true, ErrIntegerOverflow,
		},
		{
			"invalid-type", db, []byte("k-str"), []byte("0"), true, ErrInvalidValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Decr(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decr() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !bytes.Equal([]byte(strconv.Itoa(int(got))), tt.want) {
				t.Errorf("Decr() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBIncrBy(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set([]byte("k-str"), []byte("v-str"))
	_ = db.Set([]byte("k-max"), []byte(strconv.Itoa(math.MaxInt64)))

	type args struct {
		key   []byte
		delta int64
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		want    []byte
		wantErr bool
		err     error
	}{
		{
			"nil-key-init", db, args{nil, 0}, []byte("0"), false, nil,
		},
		{
			"nil-key-incr-999", db, args{nil, 999}, []byte("999"), false, nil,
		},
		{
			"normal-key-init", db, args{[]byte("k-1"), 0}, []byte("0"), false, nil,
		},
		{
			"normal-key-incr-99999", db, args{[]byte("k-1"), 99999}, []byte("99999"), false, nil,
		},
		{
			"normal-key-incr-9999999999", db, args{[]byte("k-1"), 9999999999}, []byte("10000099998"), false, nil,
		},
		{
			"int-overflow", db, args{[]byte("k-max"), 1}, []byte("0"), true, ErrIntegerOverflow,
		},
		{
			"invalid-type", db, args{[]byte("k-str"), 1}, []byte("0"), true, ErrInvalidValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.IncrBy(tt.args.key, tt.args.delta)
			if (err != nil) != tt.wantErr {
				t.Errorf("IncrBy() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !bytes.Equal([]byte(strconv.Itoa(int(got))), tt.want) {
				t.Errorf("IncrBy() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBDecrBy(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set([]byte("k-str"), []byte("v-str"))
	_ = db.Set([]byte("k-min"), []byte(strconv.Itoa(math.MinInt64)))

	type args struct {
		key   []byte
		delta int64
	}
	tests := []struct {
		name    string
		db      *KhighDB
		args    args
		want    []byte
		wantErr bool
		err     error
	}{
		{
			"nil-key-init", db, args{nil, 0}, []byte("0"), false, nil,
		},
		{
			"nil-key-decr-999", db, args{nil, 999}, []byte("-999"), false, nil,
		},
		{
			"normal-key-init", db, args{[]byte("k-1"), 0}, []byte("0"), false, nil,
		},
		{
			"normal-key-decr-99999", db, args{[]byte("k-1"), 99999}, []byte("-99999"), false, nil,
		},
		{
			"int-overflow", db, args{[]byte("k-min"), 1}, []byte("0"), true, ErrIntegerOverflow,
		},
		{
			"invalid-type", db, args{[]byte("k-str"), 1}, []byte("0"), true, ErrInvalidValueType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.DecrBy(tt.args.key, tt.args.delta)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecrBy() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !bytes.Equal([]byte(strconv.Itoa(int(got))), tt.want) {
				t.Errorf("DecrBy() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBStrLen(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	_ = db.Set([]byte(nil), []byte("zero"))
	_ = db.Set([]byte("zero"), nil)
	_ = db.Set([]byte("k-l0"), []byte(""))
	_ = db.Set([]byte("k-l1"), []byte("1"))
	_ = db.Set([]byte("k-l2"), []byte("22"))
	_ = db.Set([]byte("k-l3"), []byte("333"))

	tests := []struct {
		name string
		db   *KhighDB
		key  []byte
		want int
	}{
		{
			"nil-key", db, nil, 4,
		},
		{
			"nil-val", db, []byte("zero"), 0,
		},
		{
			"key-l0", db, []byte("k-l0"), 0,
		},
		{
			"key-l1", db, []byte("k-l1"), 1,
		},
		{
			"key-l3", db, []byte("k-l3"), 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.db.StrLen(tt.key)
			if got != tt.want {
				t.Errorf("StrLen() got = %v, want = %v", got, tt.want)
			}
		})
	}
}

func testKhighDBCount(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	for i := 1; i <= 333; i++ {
		_ = db.Set([]byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i)))
		got := db.Count()
		if got != i {
			t.Errorf("Count() got = %v, want = %v", got, i)
		}
	}
	for i := 1; i <= 333; i++ {
		_ = db.Delete([]byte(fmt.Sprintf("k-%d", i)))
		got := db.Count()
		want := 333 - i
		if got != want {
			t.Errorf("Count() got = %v, want = %v", got, want)
		}
	}
}

func testKhighDBScan(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	for i := 1; i <= 10; i++ {
		_ = db.Set([]byte(fmt.Sprintf("k+%d", i)), []byte(fmt.Sprintf("v+%d", i)))
		_ = db.Set([]byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i)))
	}

	result, err := db.Scan([]byte("k-"), "[k]{1}[-]{1}[\\d]+", 5)
	assert.Nil(t, err)
	assert.Equal(t, 10, len(result))

	result, err = db.Scan([]byte("k-"), "[k]{1}[+]{1}[\\d]+", 5)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))

	result, err = db.Scan([]byte("k"), "[k]{1}", 10)
	assert.Nil(t, err)
	assert.Equal(t, 20, len(result))

	result, err = db.Scan([]byte("k"), "", 20)
	assert.Nil(t, err)
	assert.Equal(t, 40, len(result))

	result, err = db.Scan([]byte("kk"), "", 0)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func testKhighDBExpire(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	key, val := []byte("k-ttl"), getValue16B()

	t.Run("normal", func(t *testing.T) {
		_ = db.Set(key, val)
		err := db.Expire(key, 1000*time.Millisecond)
		assert.Nil(t, err)

		time.Sleep(300 * time.Millisecond)
		got, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, val, got)

		time.Sleep(700 * time.Millisecond)
		_, err = db.Get(key)
		assert.Equal(t, ErrKeyNotFound, err)
	})

	t.Run("set-twice", func(t *testing.T) {
		_ = db.Set(key, val)
		err := db.Expire(key, 1000*time.Millisecond)
		assert.Nil(t, err)
		err = db.Expire(key, 100*time.Millisecond)
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)
		_, err = db.Get(key)
		assert.Equal(t, ErrKeyNotFound, err)
	})
}

func testKhighDBTTL(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	key, val := []byte("k-ttl"), getValue16B()
	_ = db.SetEX(key, val, 10000*time.Second)

	time.Sleep(1 * time.Second)
	ttl, err := db.TTL(key)
	ttlSecond := int(ttl / 1e3)
	assert.Nil(t, err)
	assert.Equal(t, 9998, ttlSecond)

	for i := 1; i <= 100; i++ {
		_ = db.Expire(key, time.Duration(i*1000+300)*time.Millisecond)
		ttl, err = db.TTL(key)
		ttlSecond := int(ttl / 1e3)
		assert.Nil(t, err)
		assert.Equal(t, i, ttlSecond)
	}
}

func testKhighDBPersist(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	key, val := getKey(10), getValue16B()
	missingKey := getKey(11)

	t.Run("normal", func(t *testing.T) {
		_ = db.SetEX(key, val, time.Second)
		err := db.Persist(key)
		assert.Nil(t, err)

		time.Sleep(time.Second)

		got, err := db.Get(key)
		assert.Nil(t, nil)
		assert.Equal(t, val, got)
	})

	t.Run("missing-key", func(t *testing.T) {
		err := db.Persist(missingKey)
		assert.Equal(t, ErrKeyNotFound, err)
	})
}

func testKhighDBGetStrKeys(t *testing.T, ioType IOType, mode DataIndexMode) {
	db := newKhighDB(ioType, mode)
	defer destroyDB(db)

	keys, err := db.GetStrKeys()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))

	for i := 1; i <= 100; i++ {
		_ = db.Set(getKey(i), getValue16B())
	}
	keys, err = db.GetStrKeys()
	assert.Nil(t, err)
	assert.Equal(t, 100, len(keys))

	for i := 51; i <= 100; i++ {
		_ = db.Delete(getKey(i))
	}
	keys, err = db.GetStrKeys()
	assert.Nil(t, err)
	assert.Equal(t, 50, len(keys))
}

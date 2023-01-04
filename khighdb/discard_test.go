package khighdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-28

//func init() {
//	logger.InitLogger(zapcore.DebugLevel)
//}

func TestDiscard_newDiscard(t *testing.T) {
	path := filepath.Join("/tmp", "khighdb-discard")
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	d, err := newDiscard(path, discardFileName, 8192)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, d.file.Close())
		assert.Nil(t, os.RemoveAll(path))
	}()

	assert.Equal(t, len(d.freeList), 682)
	assert.Equal(t, len(d.location), 0)
}

func TestDiscard_setTotal(t *testing.T) {
	path := filepath.Join("/tmp", "khighdb-discard")
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	d, err := newDiscard(path, discardFileName, 8192)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, d.file.Close())
		assert.Nil(t, os.RemoveAll(path))
	}()

	for i := 1; i < 300; i = i * 5 {
		d.setTotal(uint32(i), uint32(i*33))
		d.incrDiscard(uint32(i), i*10)
	}

	assert.Equal(t, len(d.freeList), 678)
	assert.Equal(t, len(d.location), 4)

	d2, err := newDiscard(path, discardFileName, 8192)
	defer func() {
		assert.Nil(t, d2.file.Close())
	}()
	assert.Nil(t, nil)
	assert.Equal(t, len(d2.freeList), 678)
	assert.Equal(t, len(d2.location), 4)
}

func TestDiscard_clear(t *testing.T) {
	path := filepath.Join("/tmp", "khighdb-discard")
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	d, err := newDiscard(path, discardFileName, 8192)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, d.file.Close())
		assert.Nil(t, os.RemoveAll(path))
	}()

	for i := 1; i < 300; i = i * 5 {
		d.setTotal(uint32(i), 333)
	}

	assert.Equal(t, len(d.freeList), 678)
	assert.Equal(t, len(d.location), 4)

	type args struct {
		fid uint32
	}
	tests := []struct {
		name string
		d    *discard
		args args
		want int
	}{
		{"clear-1", d, args{1}, 679},
		{"clear-5", d, args{5}, 680},
		{"clear-25", d, args{25}, 681},
		{"clear-125", d, args{125}, 682},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.d.clear(tt.args.fid)
			assert.Equal(t, tt.want, len(tt.d.freeList))
		})
	}
}

func TestDiscard_incrDiscard(t *testing.T) {
	path := filepath.Join("/tmp", "khighdb-discard")
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	d, err := newDiscard(path, discardFileName, 8192)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, d.file.Close())
		assert.Nil(t, os.RemoveAll(path))
	}()

	for i := 1; i < 300; i++ {
		d.setTotal(uint32(i), uint32(i*33))
	}

	for i := 1; i < 300; i++ {
		sum := 0
		for k := 1; k < 5; k++ {
			sum += k
			assert.Equal(t, d.incr(uint32(i), k), sum)
		}
	}
}

func TestDiscard_getCCL(t *testing.T) {
	path := filepath.Join("/tmp", "khighdb-discard")
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	d, err := newDiscard(path, discardFileName, 8192)
	assert.Nil(t, err)
	defer func() {
		assert.Nil(t, d.file.Close())
		assert.Nil(t, os.RemoveAll(path))
	}()

	for i := 1; i <= 625; i = i * 5 {
		d.setTotal(uint32(i), uint32(i*i))
	}
	for i := 1; i <= 625; i = i * 5 {
		d.incrDiscard(uint32(i), i)
	}

	// fid    total size   discard size   ratio
	// 1      1            1              1
	// 5      25           5              0.2
	// 25     625          25             0.04
	// 125    15625        125            0.008
	// 625    390625       625            0.0016

	t.Run("normal", func(t *testing.T) {
		ccl, err := d.getCCL(626, 0.008)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(ccl))
	})

	t.Run("filter-some", func(t *testing.T) {
		ccl, err := d.getCCL(126, 0.01)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(ccl))

		ccl, err = d.getCCL(5, 1)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(ccl))
	})
	t.Run("clear and get", func(t *testing.T) {
		d.clear(125)
		ccl, err := d.getCCL(626, 0.008)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(ccl))
	})
}

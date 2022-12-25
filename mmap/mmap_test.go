package mmap

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-25

func TestMMap(t *testing.T) {
	dir, err := ioutil.TempDir("", "rosedb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(path)
		}
	}()
	type args struct {
		fd       *os.File
		writable bool
		size     int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"normal-size", args{fd: fd, writable: true, size: 100}, false,
		},
		{
			"big-size", args{fd: fd, writable: true, size: 128 << 20}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MMap(tt.args.fd, tt.args.writable, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mmap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if int64(len(got)) != tt.args.size {
				t.Errorf("Mmap() want buf size = %d, actual = %d", tt.args.size, len(got))
			}
		})
	}
}

func TestMUnmap(t *testing.T) {
	dir, err := ioutil.TempDir("", "rosedb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(path)
		}
	}()

	buf, err := MMap(fd, true, 100)
	assert.Nil(t, err)
	err = MUnmap(buf)
	assert.Nil(t, err)
}

func TestMSync(t *testing.T) {
	dir, err := ioutil.TempDir("", "rosedb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(path)
		}
	}()

	buf, err := MMap(fd, true, 128)
	assert.Nil(t, err)
	err = MSync(buf)
	assert.Nil(t, err)
}

func TestMAdvise(t *testing.T) {
	dir, err := ioutil.TempDir("", "rosedb-mmap-test")
	assert.Nil(t, err)
	path := filepath.Join(dir, "mmap.txt")

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() {
		if fd != nil {
			_ = fd.Close()
			destroyDir(path)
		}
	}()

	buf, err := MMap(fd, true, 128)
	assert.Nil(t, err)
	err = MAdvise(buf, false)
	assert.Nil(t, err)
}

func destroyDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("remove dir err: %v", err)
	}
}

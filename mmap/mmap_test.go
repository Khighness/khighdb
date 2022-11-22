package mmap

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// @Author KHighness
// @Update 2022-11-22

var testData = []byte("0123456789ABCDEF")
var testPath = filepath.Join(os.TempDir(), "testdata")

func init() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		panic(err)
	}
	return f
}

func TestMap(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	mmap, err := Map(f, RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer mmap.Unmap()
	if !bytes.Equal(testData, mmap) {
		t.Errorf("mmapBytes(%s) != testData(%s)", mmap, testData)
	}
}

func TestReadWrite(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	mmap, err := Map(f, RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer mmap.Unmap()
	if !bytes.Equal(testData, mmap) {
		t.Errorf("mmapBytes(%s) != testData(%s)", mmap, testData)
	}

	// Update file 0123456789ABCDEF => 012345678XABCDEF
	mmap[9] = 'X'
	mmap.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, []byte("012345678XABCDEF")) {
		t.Errorf("error updating file: %s => %s", testData, fileData)
	}

	// Reset file
	mmap[9] = '9'
	mmap.Flush()
}

func TestProtFlagsANdErr(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	if _, err := Map(f, RDWR, 0); err != nil {
		t.Logf("expected error")
	}
}

func TestFlags(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	mmap, err := Map(f, COPY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer mmap.Unmap()

	mmap[9] = 'X'
	mmap.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, testData) {
		t.Errorf("file was modified")
	}
}

func TestNonZeroOffset(t *testing.T) {
	const pageSize = 65536

	// Create a 2-page sized file
	bigFilePath := filepath.Join(os.TempDir(), "nonzero")
	fileObj, err := os.OpenFile(bigFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	bigData := make([]byte, 2*pageSize, 2*pageSize)
	fileObj.Write(bigData)
	fileObj.Close()

	// Map the first page by itself
	fileObj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	m, err := MapRegion(fileObj, pageSize, RDONLY, 0, 0)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	err = m.Unmap()
	if err != nil {
		t.Error(err)
	}
	fileObj.Close()

	// Map the second page by itself
	fileObj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	m, err = MapRegion(fileObj, pageSize, RDONLY, 0, pageSize)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	err = m.Unmap()
	if err != nil {
		t.Error(err)
	}

	m, err = MapRegion(fileObj, pageSize, RDONLY, 0, 1)
	if err != nil {
		t.Log("expect error because offset is not a multiple of the page size")
	}

	fileObj.Close()
}

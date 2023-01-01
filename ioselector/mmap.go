package ioselector

import (
	"io"
	"os"

	"github.com/Khighness/khighdb/mmap"
)

// @Author KHighness
// @Update 2022-12-25

// MMapSelector represents using memory-mapped file I/O.
type MMapSelector struct {
	fd     *os.File
	buf    []byte
	bufLen int64
}

// NewMMappSelector creates a new mmap selector.
func NewMMapSelector(fileName string, fileSize int64) (IOSelector, error) {
	if fileSize <= 0 {
		return nil, ErrInvalidFileSize
	}
	file, err := openFile(fileName, fileSize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.MMap(file, true, fileSize)
	if err != nil {
		return nil, err
	}

	return &MMapSelector{
		fd:     file,
		buf:    buf,
		bufLen: int64(len(buf)),
	}, nil
}

// Write copys slice b into mapped region(buf) at offset.
func (ms MMapSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || length+offset > ms.bufLen {
		return 0, io.EOF
	}
	return copy(ms.buf[offset:], b), nil
}

// Read copys data from mapped region(buf) into slice b at offset.
func (ms MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= ms.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= ms.bufLen {
		return 0, io.EOF
	}
	return copy(b, ms.buf[offset:]), nil
}

// Sync synchronizes the mapped buffer to the file's contents on disk.
func (ms MMapSelector) Sync() error {
	return mmap.MSync(ms.buf)
}

// Close synchronizes and unmaps mapped buffer abnd close fd.
func (ms MMapSelector) Close() error {
	if err := mmap.MSync(ms.buf); err != nil {
		return err
	}
	if err := mmap.MUnmap(ms.buf); err != nil {
		return err
	}
	return ms.fd.Close()
}

// Delete deletes mapped buffer and removes file on disk.
func (ms MMapSelector) Delete() error {
	if err := mmap.MUnmap(ms.buf); err != nil {
		return err
	}
	ms.buf = nil

	if err := ms.fd.Truncate(0); err != nil {
		return err
	}
	if err := ms.fd.Close(); err != nil {
		return err
	}
	return os.Remove(ms.fd.Name())
}

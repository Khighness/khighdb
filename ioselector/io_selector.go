package ioselector

import (
	"errors"
	"os"
)

// @Author KHighness
// @Update 2022-12-25

// ErrInvalidFileSize represents invalid file size.
var ErrInvalidFileSize = errors.New("file size can not be zero or negetive")

// FilePerm is defualt permission of the newly created log file.
const FilePerm = 0644

// IOSelector defines the related functions of I/O.
type IOSelector interface {
	// Write writes a slice to log file at offset.
	// It returns the number of bytes written and an error, if any.
	Write(b []byte, offset int64) (int, error)

	// Read reads a slice from offset.
	// It returns the number of bytes read and any error encountered,
	Read(b []byte, offset int64) (int, error)

	// Sync commits the current contents of the file to stable storage.
	// Typically, this means flushing the file system's in-memory copy
	// of recently written data to disk.
	Sync() error

	// Close closes the file, rendering it unstable for I/O.
	// It will return an error if it has already been closed.
	Close() error

	// Delete deletes the file.
	// Must close it before delete, and will unmap if in MMapSelector.
	Delete() error
}

// openFile opens a file and truncates it if necessary.
func openFile(fileName string, fileSize int64) (*os.File, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fileSize {
		if err := fd.Truncate(fileSize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}

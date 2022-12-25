package ioselector

import "os"

// @Author KHighness
// @Update 2022-12-25

// FileIOSelector represents using standard file I/O.
type FileIOSelector struct {
	fd *os.File
}

// NewFileIOSelector creates a new file io selector.
func NewFileIOSelector(fileName string, fileSize int64) (IOSelector, error) {
	if fileSize <= 0 {
		return nil, ErrInvalidFileSize
	}
	file, err := openFile(fileName, fileSize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{
		fd: file,
	}, nil
}

// Write is a wrapper of os.File.WriteAt.
func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

// Read is a wrapper of os.File.ReadAt.
func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Read is a wrapper of os.File.Sync.
func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

// Read is a wrapper of os.File.Close.
func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

// Delete removes file descriptor if we do not use it anymore.
func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}

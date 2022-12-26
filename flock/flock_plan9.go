// +build plan9

package flock

import "os"

// @Author KHighness
// @Update 2022-12-26

// FileLockGuard holds a lock of file on a directory.
type FileLockGuard struct {
	fd syscall.Handle
}

// AcquireFileLock acquire the lock on the directory by syscall.Flock.
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	var (
		flag int
		mode os.FileMode
	)
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
		mode = os.ModeExclusive
	}

	file, err := os.OpenFile(path, flag, mode)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, mode|0644)
	}
	if err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil
}

// SyncDir commits the current contents of the directory to stable storage.
func SyncDir(path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	if err = fd.Sync(); err != nil {
		return fmt.Errorf("sync dir err: %v", err)
	}
	if err = fd.Close(); err != nil {
		return fmt.Errorf("close dir err: %v", err)
	}
	return nil
}

// Release releases the file lock.
func (fl *FileLockGuard) Release() error {
	return fl.fd.Close()
}

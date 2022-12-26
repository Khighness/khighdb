// +build !windows,!plan9

package flock

import (
	"fmt"
	"os"
	"syscall"
)

// @Author KHighness
// @Update 2022-12-26

// FileLockGuard holds a lock of file on a directory.
type FileLockGuard struct {
	fd syscall.Handle
}

// AcquireFileLock acquire the lock on the directory by syscall.Flock.
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	var flag = os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	file, err := os.OpenFile(path, flag, 0)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return nil, err
	}

	var how = syscall.LOCK_EX | syscall.LOCK_NB
	if readOnly {
		how = syscall.LOCK_SH | syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), how); err != nil {
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
	how := syscall.LOCK_UN | syscall.LOCK_NB
	if err := syscall.Flock(int(fl.fd.Fd()), how); err != nil {
		return err
	}
	return fl.fd.Close()
}

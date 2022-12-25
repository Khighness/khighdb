// +build darwin

package mmap

import (
	"os"
	"syscall"
	"unsafe"
)

// @Author KHighness
// @Update 2022-12-25

func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(fd.Fd(), 0, int64(size), mtype, unix.MAP_SHARED)
}

func munmap(b []byte) error {
	return unix.Munmap(b)
}

func madvise(b []byte, readahead bool) error {
	advice := unix.MADV_NORMAL
	if !readahead {
		advice = unix.MADV_NORMAL
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(advice))
	if errno != 0 {
		return errno
	}
	return nil
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}

// +build plan9

package mmap

import "os"

// @Author KHighness
// @Update 2022-12-25

func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return nil, syscall.EPLAN9
}

func munmap(b []byte) error {
	return syscall.EPLAN9
}

func madvise(b []byte, readahead bool) error {
	return syscall.EPLAN9
}

func msync(b []byte) error {
	return syscall.EPLAN9
}

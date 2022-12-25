package mmap

import (
	"os"
)

// @Author KHighness
// @Update 2022-11-17

// MMap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func MMap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// MUnmap unmaps a previously mapped slice.
func MUnmap(b []byte) error {
	return munmap(b)
}

// MAdvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. Set the readahead flag to
// false if page references are expected in random order.
func MAdvise(b []byte, readahead bool) error {
	return madvise(b, readahead)
}

// MSync would call sync on the mmapped data.
func MSync(b []byte) error {
	return msync(b)
}

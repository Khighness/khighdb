// +build !windows,!darwin,!plan9,!linux

package mmap

// @Author KHighness
// @Update 2022-12-25

func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

func munmap(b []byte) error {
	return unix.Munmap(b)
}

func madvise(b []byte, readahead bool) {
	flags := unix.MADV_NORMAL
	if !readahead {
		flags = unix.MADV_NORMAL
	}
	return unix.Madvise(b, flags)
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}

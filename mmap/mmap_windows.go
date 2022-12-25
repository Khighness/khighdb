// +build windows

package mmap

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// @Author KHighness
// @Update 2022-12-25

// mmap on Windows system is a two-step process.
// First, we call windows.CreateFileMapping to get a handle.
// Then, we call windows.MapviewToFile to get an actual pointer into memory.
// Because we want to emulate a POSIX_style mmap, we don't want to expose
// the handle -- only the pointer. We also want to return only a byte slice.

func mmap(fd *os.File, write bool, size int64) ([]byte, error) {
	protect := syscall.PAGE_READONLY
	access := syscall.FILE_MAP_READ

	if write {
		protect = syscall.PAGE_READWRITE
		access = syscall.FILE_MAP_WRITE
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	// In windows, we cannot mmap a file more than it's actual size.
	// So truncate the file to the file of the mmap.
	if fi.Size() < size {
		if err := fd.Truncate(size); err != nil {
			return nil, fmt.Errorf("truncate: %s", err)
		}
	}

	// Open a file mapping handle.
	sizelo := uint32(size >> 32)
	sizehi := uint32(size) & 0xffffffff

	handler, err := syscall.CreateFileMapping(syscall.Handle(fd.Fd()), nil,
		uint32(protect), sizelo, sizehi, nil)
	if err != nil {
		return nil, os.NewSyscallError("CreateFileMapping", err)
	}

	// Create the memory mmap.
	addr, err := syscall.MapViewOfFile(handler, uint32(access), 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", err)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(handler)); err != nil {
		return nil, os.NewSyscallError("CLoseHandle", err)
	}

	// Slice memory layout
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(size), int(size)}

	// Use unsafe to trun sl into a byte slice
	data := *(*[]byte)(unsafe.Pointer(&sl))

	return data, nil
}

func munmap(b []byte) error {
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&b[0])))
}

func madvise(b []byte, readahead bool) error {
	return nil
}

func msync(b []byte) error {
	return syscall.FlushViewOfFile(uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
}

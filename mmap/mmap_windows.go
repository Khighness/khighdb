// +build windows

package mmap

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/windows"
)

// @Author KHighness
// @Update 2022-11-17

// mmap on Windows system is a two-step process.
// First, we call windows.CreateFileMapping to get a handle.
// Then, we call windows.MapviewToFile to get an actual pointer into memory.
// Because we want to emulate a POSIX_style mmap, we don't want to expose
// the handle -- only the pointer. We also want to return only a byte slice.

// We keep this map so that we can get back the original handle from the memory address.

type addrInfo struct {
	file      windows.Handle
	mapview   windows.Handle
	writeable bool
}

var handleLock sync.Mutex
var handleMap = map[uintptr]*addrInfo{}
var ErrUnknownBaseAddress = errors.New("unknown base address")

func mmap(len int, prot, flags, hfile uintptr, off int64) ([]byte, error) {
	flProtect := uint32(windows.PAGE_READONLY)
	dwDesireAccess := uint32(windows.FILE_MAP_READ)
	writeable := false
	switch {
	case prot&COPY != 0:
		flProtect = windows.PAGE_WRITECOPY
		dwDesireAccess = windows.FILE_MAP_COPY
		writeable = true
	case prot&RDWR != 0:
		flProtect = windows.PAGE_READWRITE
		dwDesireAccess = windows.FILE_MAP_WRITE
		writeable = true
	}
	if prot&EXEC != 0 {
		flProtect <<= 4
		dwDesireAccess |= windows.FILE_MAP_EXECUTE
	}

	// The maximum size is the area of the file, starting from 0,
	// that we wish to allow to be mappable. It is the sum of the
	// length the user requested, plus the offset where that length
	// is starting from. This does not map the data into memory.
	maxSizeHigh := uint32((off + int64(len)) >> 32)
	maxSizeLow := uint32((off + int64(len)) & 0xFFFFFFFF)
	h, errno := windows.CreateFileMapping(windows.Handle(hfile), nil, flProtect, maxSizeHigh, maxSizeLow, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Actually map a view of the data into memory. The view's size
	// is the length the user requested.
	fileOffsetHigh := uint32(off >> 32)
	fileOffsetLow := uint32(off & 0xFFFFFFFF)
	addr, errno := windows.MapViewOfFile(h, dwDesireAccess, fileOffsetHigh, fileOffsetLow, uintptr(len))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}
	handleLock.Lock()
	handleMap[addr] = &addrInfo{
		file:      windows.Handle(hfile),
		mapview:   h,
		writeable: writeable,
	}
	handleLock.Unlock()

	m := MMap{}
	dh := m.header()
	dh.Data = addr
	dh.Len = len
	dh.Cap = dh.Len

	return m, nil
}

func (m MMap) flush() error {
	addr, l := m.addrLen()
	errno := windows.FlushViewOfFile(addr, l)
	if errno != nil {
		return os.NewSyscallError("FlushViewOfFile", errno)
	}

	handleLock.Lock()
	defer handleLock.Unlock()
	handle, ok := handleMap[addr]
	if !ok {
		return ErrUnknownBaseAddress
	}

	if handle.writeable {
		if errno := windows.FlushFileBuffers(handle.file); errno != nil {
			return os.NewSyscallError("FlushFileBuffers", errno)
		}
	}

	return nil
}

func (m MMap) lock() error {
	addr, l := m.addrLen()
	errno := windows.VirtualLock(addr, l)
	return os.NewSyscallError("VirtualLock", errno)
}

func (m MMap) unlock() error {
	addr, l := m.addrLen()
	errno := windows.VirtualUnlock(addr, l)
	return os.NewSyscallError("VirtualUnlock", errno)
}

func (m MMap) unmap() error {
	err := m.flush()
	if err != nil {
		return err
	}

	addr := m.header().Data
	// Lock the UnmapViewOfFile along with the handleMap deletion.
	// As soon as we unmap the view, the OS is free to give the same
	// addr to another new map. We don't want another goroutine to
	// insert and remove the same addr into handleMap while we're
	// trying to remove our old addr/handle pair.
	handleLock.Lock()
	defer handleLock.Unlock()
	err = windows.UnmapViewOfFile(addr)
	if err != nil {
		return err
	}

	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would' ve errored above
		return ErrUnknownBaseAddress
	}
	delete(handleMap, addr)

	e := windows.CloseHandle(windows.Handle(handle.mapview))
	return os.NewSyscallError("CloseHandle", e)
}

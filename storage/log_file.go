package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Khighness/khighdb/ioselector"
)

// @Author KHighness
// @Update 2022-12-25

var (
	// ErrInvalidCrc represents invalid crc.
	ErrInvalidCrc = errors.New("logfile: invalid crc")
	// ErrWriteSizeNotEqual represents write size is not equal yp entry size.
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal yp entry size")
	// ErrEndOfEntry represents end of entry in log file.
	ErrEndOfEntry = errors.New("logfile: end of entry in log file")
	// ErrUnsupportedIOType represents unsupported io type, only support mmap and fileIO now.
	ErrUnsupportedIOType = errors.New("logfile: unsupported io type")
	// ErrUnsupportedLogFileType represents unsupported log file type, only WAL and ValueLog now.
	ErrUnsupportedLogFileType = errors.New("logfile: unsupported log file type")
)

const (
	// InitialLogField intializes log file ig with 0.
	InitialLogField = 0

	// FilePrefix defines the log file prefix.
	FilePrefix = "log."
)

// FileType represents represents deifferent types of log file: wal and value log.
type FileType int8

const (
	Strs FileType = iota
	List
	Hash
	Sets
	ZSet
)

var (
	FileNamesMap = map[FileType]string{
		Strs: "log.strs.",
		List: "log.list.",
		Hash: "log.hash.",
		Sets: "log.sets.",
		ZSet: "log.zset.",
	}

	FileTypesMap = map[string]FileType{
		"strs": Strs,
		"list": List,
		"hash": Hash,
		"sets": Sets,
		"zset": ZSet,
	}
)

// IOType represents
type IOType int8

const (
	FileIO IOType = iota
	MMap
)

// LogFile is an abstraction of a disk file, entry's read and write will go through it.
type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IoSelector ioselector.IOSelector
}

// OpenLogFile opens an existing log file or creates a new log file.
func OpenLogFile(path string, fid uint32, fsize int64, ftype FileType, ioType IOType) (logFile *LogFile, err error) {
	logFile = &LogFile{Fid: fid}
	fileName, err := logFile.generateLogFileName(path, fid, ftype)
	if err != nil {
		return nil, err
	}

	var ioSelector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if ioSelector, err = ioselector.NewFileIOSelector(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if ioSelector, err = ioselector.NewMMapSelector(fileName, fsize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIOType
	}

	logFile.IoSelector = ioSelector
	return
}

// ReadLogEntry reads a LogEntry from log file at offset.
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// Read entry meta.
	metaBuf, err := lf.readBytes(offset, MaxMetaSize)
	if err != nil {
		return nil, 0, err
	}
	meta, size := decodeMeta(metaBuf)
	if meta.crc32 == 0 && meta.keySize == 0 && meta.valSize == 0 {
		return nil, 0, ErrEndOfEntry
	}
	e := &LogEntry{
		ExpiredAt: meta.expiredAt,
		Type:      meta.typ,
	}
	keySize, valSize := int64(meta.keySize), int64(meta.valSize)
	var entrySize = size + keySize + valSize

	// Read entry key and value.
	if keySize > 0 || valSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, keySize+valSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:keySize]
		e.Value = kvBuf[keySize:]
	}

	// Check crc32.
	if crc := getEntryCrc(e, metaBuf[crc32.Size:size]); crc != meta.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

// Read reads a byte slice in the log file at offset
func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IoSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// Write writes a byte alice at the end oflog file.
// Returns an error, if any.
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

// Sync commits the current contents of the log file to stable storage.
func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

// Close closes current logh file.
func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

// Delete deletes current log file.
// This is an irreversible operation. Please consider carefully.
func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}

// readBytes read the specified length of bytes at offset.
func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

// generateLogFileName generates log file name according to the file type.
func (lf *LogFile) generateLogFileName(path string, fid uint32, ftype FileType) (name string, err error) {
	if _, ok := FileNamesMap[ftype]; !ok {
		return "", ErrUnsupportedLogFileType
	}

	fname := FileNamesMap[ftype] + fmt.Sprintf("%09.d", fid)
	name = filepath.Join(path, fname)
	return
}

package store

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/khighness/khighdb/mmap"
)

// @Author KHighness
// @Update 2022-11-16

const (
	// FilePerm is default permission of the newly created db file.
	FilePerm = 0644

	// PathSeparator is the default path separator of current system.
	PathSeparator = string(os.PathSeparator)

	// mergeDir is a temporary_directory, only exists when merging,
	mergeDir = "khighdb_merge"
)

var (
	// DBFileFormatNames records the name format of the db files.
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	// DBFileSuffixName records the suffix names of the db files.
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

var (
	// ErrEmptyEntry represents the entry is empty.
	ErrEmptyEntry = errors.New("store|dbfile: entry or the key of entry is empty")
	// ErrEntryTooLarge represents
	ErrEntryTooLarge = errors.New("store|dbfile: entry is too large to store in mmap mode")
)

// FileRWMethod represents read and write method for db file.
type FileRWMethod uint8

const (
	// Standard indicates that read and write data file by system standard IO.
	Standard FileRWMethod = iota

	// MMap indicates that read and write data file by mmap.
	MMap
)

// DBFile defines the structure of data file in khighdb.
type DBFile struct {
	Id     uint32
	Path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

// NewDBFile creates a new file db file, truncate the file if tw method is mmap.
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, Path: path, Offset: stat.Size(), method: method}

	if method == Standard {
		df.File = file
	} else {
		if err := file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

// Read date from the db file, offset is the start position of reading.
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte

	// read entry header info.
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return nil, err
	}

	if e, err = Decode(buf); err != nil {
		return nil, err
	}

	// read key if necessary.
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return nil, err
		}
		e.Meta.Key = key
	}

	// read value if necessary.
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return nil, err
		}
		e.Meta.Value = val
	}

	// read extra info if necessary.
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var extra []byte
		if extra, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return nil, err
		}
		e.Meta.Extra = extra
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == Standard {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

func (df *DBFile) Write(e *Entry) (err error) {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method, offset := df.method, df.Offset
	var encVal []byte
	if encVal, err = Encode(e); err != nil {
		return
	}

	if method == Standard {
		if _, err = df.File.WriteAt(encVal, offset); err != nil {
			return
		}
	}
	if method == MMap {
		if offset+int64(len(encVal)) > int64(len(df.mmap)) {
			return ErrEntryTooLarge
		}
		copy(df.mmap[offset:], encVal)
	}

	df.Offset += int64(e.Size())
	return
}

// Close closes the db file, sync means whether to persist data before closing.
func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

// Sync persists db file into disk.
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}
	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return
}

// Build loads all db files from disk.
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	// build merged files if necessary.
	// merge path is a sub directory in path.
	var (
		mergedFiles map[uint16]map[uint32]*DBFile
		mErr        error
	)
	for _, d := range dir {
		if d.IsDir() && strings.Contains(d.Name(), mergeDir) {
			mergePath := path + string(os.PathSeparator) + d.Name()
			if mergedFiles, _, mErr = Build(mergePath, method, blockSize); mErr != nil {
				return nil, nil, mErr
			}
		}
	}

	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			// get file's type.
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// load all the db files.
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)
		var activeFileID uint32 = 0

		if len(fileIDs) > 0 {
			activeFileID = uint32(fileIDs[len(fileIDs)-1])

			length := len(fileIDs) - 1
			if strings.Contains(path, mergeDir) {
				length++
			}
			for i := 0; i < length; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileID
	}

	// merged files are also archived files.
	if mergedFiles != nil {
		for dType, file := range archFiles {
			if mergedFile, ok := mergedFiles[dType]; ok {
				for id, f := range mergedFile {
					file[id] = f
				}
			}
		}
	}
	return archFiles, activeFileIds, nil
}

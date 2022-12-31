package khighdb

import (
	"encoding/binary"
	"errors"
	"go.uber.org/zap"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/khighness/khighdb/data/art"
	"github.com/khighness/khighdb/data/zset"
	"github.com/khighness/khighdb/flock"
	"github.com/khighness/khighdb/storage"
	"github.com/khighness/khighdb/util"
)

// @Author KHighness
// @Update 2022-12-26

var (
	// ErrKeyNotFound represents key is not found.
	ErrKeyNotFound = errors.New("key not found")
	// ErrLogFileNotFound represents log file is not found.
	ErrLogFileNotFound = errors.New("log file not found")
	// ErrWrongNumberOfArgs represents the number of arguments is invalid.
	ErrInvalidNumberOfArgs = errors.New("invalid number of arguments")
	// ErrIntegerOverflow represents the result after increment or decrement overflows int64 limitations.
	ErrIntegerOverflow = errors.New("increment or decrement overflow")
	// ErrInvalidValueType represents the type of value is invalid.
	ErrInvalidValueType = errors.New("value is not an integer")
	// ErrIndexOutOfRange represents the index is out of range,
	ErrIndexOutOfRange = errors.New("index is out of range")
	// ErrLogFileGCRunning represents log file gc is running.
	ErrLogFileGCRunning = errors.New("log file gc is running, retry later")
)

const (
	logFileTypeNum   = 5
	encodeHeaderSize = 10
	initialListSeq   = math.MaxUint32 / 2
	discardFilePath  = "DISCARD"
	lockFileName     = "FLOCK"
)

// KhighDB defines the structure of KhighDB.
type KhighDB struct {
	activeLogFiles   map[DataType]*storage.LogFile
	archivedLogFiles map[DataType]archivedFiles
	fidMap           map[DataType][]uint32
	discards         map[DataType]*discard
	options          Options
	strIndex         *strIndex
	listIndex        *listIndex
	hashIndex        *hashIndex
	setIndex         *setIndex
	zsetIndex        *zsetIndex
	mu               sync.RWMutex
	fileLock         *flock.FileLockGuard
	closed           uint32
	gcState          int32
}

type (
	archivedFiles map[uint32]*storage.LogFile

	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}

	indexNode struct {
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
		value     []byte // this is nil in KeyOnlyMemMode
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	listIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{
		idxTree: art.NewART(),
		mu:      new(sync.RWMutex),
	}
}

func newListIndex() *listIndex {
	return &listIndex{
		trees: make(map[string]*art.AdaptiveRadixTree),
		mu:    new(sync.RWMutex),
	}
}

func newHashIndex() *hashIndex {
	return &hashIndex{
		trees: make(map[string]*art.AdaptiveRadixTree),
		mu:    new(sync.RWMutex),
	}
}

func newSetIndex() *setIndex {
	return &setIndex{
		mu:      new(sync.RWMutex),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
	}
}

func newZSetIndex() *zsetIndex {
	return &zsetIndex{
		mu:      new(sync.RWMutex),
		indexes: zset.New(),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
	}
}

// Open a KhighDB instance.
func Open(options Options) (*KhighDB, error) {
	// Create the directory if the path does not exist.
	if !util.PathExist(options.DBPath) {
		if err := os.MkdirAll(options.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Acquire file lock to prevent multiple process from
	// access the same directory.
	lockPath := filepath.Join(options.DBPath, lockFileName)
	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &KhighDB{
		activeLogFiles:   make(map[DataType]*storage.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		options:          options,
		strIndex:         newStrsIndex(),
		listIndex:        newListIndex(),
		hashIndex:        newHashIndex(),
		setIndex:         newSetIndex(),
		zsetIndex:        newZSetIndex(),
		fileLock:         lockGuard,
	}

	// Initialize the discard file.
	if err = db.initDiscard(); err != nil {
		return nil, err
	}
	// Load the log files from disk.
	if err = db.loadLogFiles(); err != nil {
		return nil, err
	}
	// Load indexes from log files.
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	go db.handleLogFileGC()
	return db, nil
}

// Close closes the KhighDB instance and saves relative configs.
func (db *KhighDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Sync and close the active log file.
	for _, activeLogFile := range db.activeLogFiles {
		_ = activeLogFile.Sync()
		_ = activeLogFile.Close()
	}

	// Sync and close the archived log files.
	for _, archivedLogFiles := range db.archivedLogFiles {
		for _, archivedLogFile := range archivedLogFiles {
			_ = archivedLogFile.Sync()
			_ = archivedLogFile.Close()
		}
	}

	// Close discard channel.
	for _, discard := range db.discards {
		discard.closeChan()
	}

	// Set db close state
	atomic.StoreUint32(&db.closed, 1)
	// Reset db index.
	db.strIndex = nil
	db.hashIndex = nil
	db.listIndex = nil
	db.setIndex = nil
	db.zsetIndex = nil

	// Release the file lock.
	if db.fileLock != nil {
		if err := db.fileLock.Release(); err != nil {
			zap.L().Error("failed to release file lock", zap.Error(err))
		}
	}

	zap.L().Info("KhighDB is closed successfully")
	return nil
}

func (db *KhighDB) initDiscard() error {
	discardPath := filepath.Join(db.options.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	for i := String; i < logFileTypeNum; i++ {
		name := storage.FileNamesMap[storage.FileType(i)] + discardFileName
		discard, err := newDiscard(discardPath, name, db.options.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = discard
	}
	db.discards = discards
	return nil
}

func (db *KhighDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	fileInfos, err := ioutil.ReadDir(db.options.DBPath)
	if err != nil {
		return err
	}

	fidMap := make(map[DataType][]uint32)
	for _, file := range fileInfos {
		if strings.HasPrefix(file.Name(), storage.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			dataType := DataType(storage.FileTypesMap[splitNames[1]])
			fidMap[dataType] = append(fidMap[dataType], uint32(fid))
		}
	}
	db.fidMap = fidMap

	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// Load log files in order.
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		options := db.options
		for i, fid := range fids {
			fileType, ioType := storage.FileType(dataType), storage.IOType(options.IoType)
			logFile, err := storage.OpenLogFile(options.DBPath, fid, options.LogFileSizeThreshold, fileType, ioType)
			if err != nil {
				return err
			}
			// Lastest one is active log file.
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = logFile
			} else {
				db.archivedLogFiles[dataType][fid] = logFile
			}
		}
	}

	return nil
}

func (db *KhighDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	options := db.options
	fileType, ioType := storage.FileType(dataType), storage.IOType(options.IoType)
	logFile, err := storage.OpenLogFile(options.DBPath, storage.InitialLogField, options.LogFileSizeThreshold, fileType, ioType)
	if err != nil {
		return err
	}

	db.discards[dataType].setTotal(logFile.Fid, uint32(options.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = logFile
	return nil
}

// openKeyValueMemMode judges if db's index mode is equal to KeyValueMemMode.
// Returning true represents both read and write operations of value are performed
// in memory without disk intervention.
func (db *KhighDB) openKeyValueMemMode() bool {
	return db.options.IndexMode == KeyValueMemMode
}

func (db *KhighDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *KhighDB) getActiveLogFile(dataType DataType) *storage.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

func (db *KhighDB) getArchivedLogFile(dataType DataType, fid uint32) *storage.LogFile {
	var logFile *storage.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.activeLogFiles[dataType] != nil {
		logFile = db.archivedLogFiles[dataType][fid]
	}
	return logFile
}

func (db *KhighDB) encodeKey(key, subkey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var headerSize int
	headerSize += binary.PutVarint(header[headerSize:], int64(len(key)))
	headerSize += binary.PutVarint(header[headerSize:], int64(len(subkey)))
	keyLength := len(key) + len(subkey)
	if keyLength > 0 {
		buf := make([]byte, headerSize+keyLength)
		copy(buf[:headerSize], header[:headerSize])
		copy(buf[headerSize:headerSize+len(key)], key)
		copy(buf[headerSize+len(key):], subkey)
		return buf
	}
	return header[:headerSize]
}

func (db *KhighDB) decodeKey(key []byte) ([]byte, []byte) {
	var headerSize int
	keyLength, keySize := binary.Varint(key[headerSize:])
	headerSize += keySize
	_, subkeySize := binary.Varint(key[headerSize:])
	headerSize += subkeySize
	subkeyIndex := headerSize + int(keyLength)
	return key[headerSize:subkeyIndex], key[subkeyIndex:]
}

// writeLogEntry writes a logEntry to the active logFile corresponding to data type.
func (db *KhighDB) writeLogEntry(ent *storage.LogEntry, dataType DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}

	options := db.options
	entBuf, entSize := storage.EncodeEntry(ent)

	// Checks if the log file exceeds threshold.
	if activeLogFile.WriteAt+int64(entSize) > options.LogFileSizeThreshold {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}

		db.mu.Lock()

		// Save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// Open a new log file.
		fileType, ioType := storage.FileType(dataType), storage.IOType(options.IoType)
		logFile, err := storage.OpenLogFile(options.DBPath, activeFileId+1, options.LogFileSizeThreshold, fileType, ioType)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		db.discards[dataType].setTotal(logFile.Fid, uint32(options.LogFileSizeThreshold))
		db.activeLogFiles[dataType] = logFile
		activeLogFile = logFile
		db.mu.Unlock()
	}

	// Write entry and sync if necessary.
	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	if options.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{
		fid:    activeLogFile.Fid,
		offset: writeAt,
	}, nil
}

package khighdb

import (
	"errors"
	"github.com/khighness/khighdb/data/art"
	"github.com/khighness/khighdb/data/zset"
	"github.com/khighness/khighdb/storage"
	"github.com/khighness/khighdb/util"
	"math"
	"sync"
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
}

type (
	archivedFiles map[uint32]*storage.LogFile

	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}

	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
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

	zsetInex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
)

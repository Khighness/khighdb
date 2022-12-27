package khighdb

import (
	"errors"
	"github.com/khighness/khighdb/ioselector"
	"sync"
)

// @Author KHighness
// @Update 2022-12-27

const (
	discardRecordSize       = 12
	discardFileSize   int64 = 8 << 10
	discardFileName         = "discard"
)

// ErrDiscardNoSpace represents no enough space for discard file.
var ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")

// discard is used to record total size and discarded size in a log file.
// Mainly for log files compaction.
type discard struct {
	sync.Mutex
	once     *sync.Once
	valChan  chan *indexNode
	file     ioselector.IOSelector
	freeList []int64          // contains file offset that can be allocated
	location map[uint32]int64 // offset of each fid
}

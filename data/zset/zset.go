package zset

import (
	"github.com/khighness/khighdb/storage"
	"github.com/khighness/khighdb/util"
)

// @Author KHighness
// @Update 2022-12-25

const (
	maxLevel    = 32
	probability = 0.25
)

type EncodeKey func(key, subKey []byte) []byte

type (
	// SortedSet defines the structure of sorted set.
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	// SortedSetNode defines the structure of sorted set node.
	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}
)

type (
	sklLevel struct {
		forward *sklNode
		span    uint64
	}

	sklNode struct {
		member   string
		score    float64
		backward *sklNode
		level    []*sklLevel
	}

	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int64
		level  int16
	}
)

// New creates a new sorted set.
func New() *SortedSet {
	return &SortedSet{
		record: make(map[string]*SortedSetNode),
	}
}

func (z *SortedSet) IterateAndSend(chn chan *storage.LogEntry, encode EncodeKey) {
	for key, ss := range z.record {
		zsetKey := []byte(key)
		if ss.skl.head == nil {
			return
		}
		for e := ss.skl.head.level[0].forward; e != nil; e = e.level[0].forward {
			scoreBuf := []byte(util.Float64ToStr(e.score))
			encKey := encode(zsetKey, scoreBuf)
			chn <- &storage.LogEntry{Key: encKey, Value: []byte(e.member)}
		}
	}
	return
}

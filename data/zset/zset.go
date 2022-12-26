package zset

import (
	"math"

	"github.com/khighness/khighdb/storage"
	"github.com/khighness/khighdb/util"
)

// @Author KHighness
// @Update 2022-12-25

type EncodeKey func(key, subKey []byte) []byte

type (
	// SortedSet defines the structure of sorted set.
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	// SortedSetNode defines the structure of sorted set node.
	SortedSetNode struct {
		dict map[string]*SklNode
		skl  *SkipList
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
		for e := ss.skl.head.level[0].backward; e != nil; e = e.level[0].backward {
			scoreBuf := []byte(util.Float64ToStr(e.score))
			encKey := encode(zsetKey, scoreBuf)
			chn <- &storage.LogEntry{Key: encKey, Value: []byte(e.member)}
		}
	}
	return
}

// ZAdd adds the specified member with the specified score to the sprted set stored at key.
func (z *SortedSet) ZAdd(key string, score float64, member string) {
	if !z.exist(key) {
		node := &SortedSetNode{
			dict: make(map[string]*SklNode),
			skl:  NewSkipList(),
		}
		z.record[key] = node
	}

	item := z.record[key]
	v, exist := item.dict[member]

	var node *SklNode
	if exist {
		if score != v.score {
			item.skl.Delete(v.score, member)
			node = item.skl.Insert(score, member)
		}
	} else {
		node = item.skl.Insert(score, member)
	}

	if node != nil {
		item.dict[member] = node
	}
}

// ZScore returns the score of member in the sorted set at key.
func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	if !z.exist(key) {
		return
	}

	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}

	return true, node.score
}

// ZCard returns the sorted set cardinality (number of elements) of he sorted set stored at key.
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	return len(z.record[key].dict)
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
// If the key or the member does not exist, -1 will be returned.
func (z *SortedSet) ZRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.GetRank(v.score, member)
	rank--

	return rank
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.GetRank(v.score, member)
	return z.record[key].skl.length - rank
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sroted set with the specified member as its sole member is created.
func (z *SortedSet) ZIncrBy(key string, member string, increment float64) float64 {
	if z.exist(key) {
		v, exist := z.record[key].dict[member]
		if exist {
			increment += v.score
		}
	}

	z.ZAdd(key, increment, member)
	return increment
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (z *SortedSet) ZRange(key string, start, stop int64) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, false)
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (z *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, true)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with same score.
func (z *SortedSet) ZRevRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, false)
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with same score.
func (z *SortedSet) ZRevRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, true)
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}

	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.Delete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}

	return false
}

// ZGetByRank gets the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max
// (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return nil
	}

	skl := z.record[key].skl
	return skl.ScoreRange(min, max)
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between min and max
// (including elements with score equal to min or max).
// The elements are considered to be ordered from high to low scores.
func (z *SortedSet) ZRevScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return nil
	}

	skl := z.record[key].skl
	return skl.RevScoreRange(min, max)
}

// ZKeyExists checks if the key exists in the sorted set,
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// ZClear clears the key in the sorted set.
func (z *SortedSet) ZClear(key string) {
	if z.exist(key) {
		delete(z.record, key)
	}
}

func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

func (z *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	skl := z.record[key].skl
	length := skl.length

	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += length
	}
	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *SklNode
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.GetByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].backward
		if start > 0 {
			node = skl.GetByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.forward
		} else {
			node = node.level[0].backward
		}
	}
	return
}

func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {
	skl := z.record[key].skl
	if rank < 0 || rank > skl.length {
		return "", math.MinInt64
	}

	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}

	n := skl.GetByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}
	node := z.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}

	return node.member, node.score
}

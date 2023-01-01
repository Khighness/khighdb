package zset

import (
	"testing"

	"github.com/Khighness/khighdb/util"
	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-12-26

func TestNewSkipList(t *testing.T) {
	skl := NewSkipList()
	assert.NotNil(t, skl)
}

func TestSkipList_Insert(t *testing.T) {
	skl := NewSkipList()
	skl.Insert(99, "K")
	skl.Insert(66, "H")
	skl.Insert(77, "I")
	assert.Equal(t, int64(3), skl.Size())
}

func TestSkipList_Delete(t *testing.T) {
	skl := NewSkipList()
	skl.Insert(99, "K")
	skl.Insert(66, "H")
	skl.Insert(77, "I")
	skl.Delete(99, "K")
	skl.Delete(66, "H")
	assert.Equal(t, int64(1), skl.Size())
}

func TestSkipList_GetRank(t *testing.T) {
	skl := NewSkipList()
	skl.Insert(99, "K")
	skl.Insert(66, "H")
	skl.Insert(77, "I")
	assert.Equal(t, int64(1), skl.GetRank(66, "H"))
	assert.Equal(t, int64(2), skl.GetRank(77, "I"))
	assert.Equal(t, int64(3), skl.GetRank(99, "K"))
	assert.Equal(t, int64(0), skl.GetRank(1, "K"))
}

func TestSkipList_GetByRank(t *testing.T) {
	skl := NewSkipList()
	skl.Insert(99, "K")
	skl.Insert(66, "H")
	skl.Insert(77, "I")
	node2 := skl.GetByRank(2)
	assert.NotNil(t, node2)
	assert.Equal(t, float64(77), node2.score)
	assert.Equal(t, "I", node2.member)
	node4 := skl.GetByRank(4)
	assert.Nil(t, node4)
}

func TestSkipList_ScoreRange(t *testing.T) {
	skl := NewSkipList()
	for i := 1; i <= 100; i++ {
		skl.Insert(float64(i), util.Float64ToStr(float64(i)))
	}
	res := skl.ScoreRange(3, 33)
	t.Log(res)
	assert.Equal(t, 62, len(res))
}

func TestSkipList_RevScoreRange(t *testing.T) {
	skl := NewSkipList()
	for i := 1; i <= 100; i++ {
		skl.Insert(float64(i), util.Float64ToStr(float64(i)))
	}
	res := skl.RevScoreRange(3, 33)
	t.Log(res)
	assert.Equal(t, 62, len(res))
}

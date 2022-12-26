package zset

import "math/rand"

// @Author KHighness
// @Update 2022-12-26

const (
	// maxLevel is the max level of skip list.
	maxLevel    = 32
	probability = 0.25
)

type (
	// SklLevel defines the structure of skip list level.
	SklLevel struct {
		backward *SklNode
		span     uint64
	}

	// SklNode defines the structure of skip list node.
	SklNode struct {
		member  string
		score   float64
		forward *SklNode
		level   []*SklLevel
	}

	// SkipList defines the structure of skip list.
	SkipList struct {
		head   *SklNode
		tail   *SklNode
		length int64
		level  int16
	}
)

// newSklNode creates a new skip list node internally.
func newSklNode(level int16, score float64, member string) *SklNode {
	node := &SklNode{
		member: member,
		score:  score,
		level:  make([]*SklLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(SklLevel)
	}

	return node
}

// NewSkipList creates a new skip list.
func NewSkipList() *SkipList {
	return &SkipList{
		level: 1,
		head:  newSklNode(maxLevel, 0, ""),
	}
}

// randomLevel generates a random level.
func randomLevel() int16 {
	var level int16 = 1
	for level < maxLevel {
		if rand.Float64() < probability {
			break
		}
		level++
	}
	return level
}

// Size returns the length of skip list.
func (skl *SkipList) Size() int64 {
	return skl.length
}

// insert adds a member with score to skip list.
func (skl *SkipList) Insert(score float64, member string) *SklNode {
	// In each level, find the forward node
	// (the first node whose score is greater than the node to be inserted)
	front := make([]*SklNode, maxLevel)
	// In each level, record the node's rank.
	ranks := make([]uint64, maxLevel)

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		if i == skl.level-1 {
			ranks[i] = 0
		} else {
			ranks[i] = ranks[i+1]
		}

		if p.level[i] != nil {
			for p.level[i].backward != nil &&
				(p.level[i].backward.score < score ||
					(p.level[i].backward.score == score && p.level[i].backward.member < member)) {
				ranks[i] += p.level[i].span
				p = p.level[i].backward
			}
		}
		front[i] = p
	}

	level := randomLevel()
	if level > skl.level {
		for i := skl.level; i < level; i++ {
			ranks[i] = 0
			front[i] = skl.head
			front[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	newNode := newSklNode(level, score, member)
	for i := int16(0); i < level; i++ {
		newNode.level[i].backward = front[i].level[i].backward
		front[i].level[i].backward = newNode

		newNode.level[i].span = front[i].level[i].span - (ranks[0] - ranks[i])
		front[i].level[i].span = (ranks[0] - ranks[i]) + 1
	}
	for i := level; i < skl.level; i++ {
		front[i].level[i].span++
	}

	// In level 0, maintain the node's relationship.
	if front[0] == skl.head {
		newNode.forward = nil
	} else {
		newNode.forward = front[0]
	}
	if newNode.level[0].backward != nil {
		newNode.level[0].backward.forward = newNode
	} else {
		skl.tail = newNode
	}

	skl.length++
	return newNode
}

// delete removes nodes according to score and member.
func (skl *SkipList) Delete(score float64, member string) {
	front := make([]*SklNode, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].backward != nil &&
			(p.level[i].backward.score < score ||
				(p.level[i].backward.score == score && p.level[i].backward.member < member)) {
			p = p.level[i].backward
		}
		front[i] = p
	}

	p = p.level[0].backward
	if p != nil && score == p.score && p.member == member {
		skl.deleteNode(p, front)
		return
	}
}

// deleteNode removes node in every level and updates other nodes's span.
func (skl *SkipList) deleteNode(p *SklNode, front []*SklNode) {
	for i := int16(0); i < skl.level; i++ {
		if front[i].level[i].backward == p {
			front[i].level[i].span += p.level[i].span - 1
			front[i].level[i].backward = p.level[i].backward
		} else {
			front[i].level[i].span--
		}
	}

	if p.level[0].backward != nil {
		p.level[0].backward.forward = p.forward
	} else {
		skl.tail = p.forward
	}

	for skl.level > 1 && skl.head.level[skl.level-1].backward == nil {
		skl.level--
	}

	skl.length--
}

// getRank returns the rank of node according to score and member.
// // The rank (or index) is 1-based, which means that the member
// with the lowest score has rank 1.
// If the node corresponding to the score and member is not found,
// then 0 will be returned.
func (skl *SkipList) GetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].backward != nil &&
			(p.level[i].backward.score < score ||
				(p.level[i].backward.score == score && p.level[i].backward.member <= member)) {
			rank += p.level[i].span
			p = p.level[i].backward
		}

		if p.member == member {
			return int64(rank)
		}
	}

	return 0
}

// getByRank returns node according to the rank.
// If the node corresponding to the rank is not found,
// then nil will be returned.
func (skl *SkipList) GetByRank(rank uint64) *SklNode {
	var traversed uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].backward != nil && (traversed+p.level[i].span) <= rank {
			traversed += p.level[i].span
			p = p.level[i].backward
		}
		if traversed == rank {
			return p
		}
	}

	return nil
}

// ScoreRange returns all the elements whose score is between min and max.
// The elements are consideres to be ordered from low to high scores.
func (skl *SkipList) ScoreRange(min, max float64) (val []interface{}) {
	minScore, maxScore := skl.head.level[0].backward.score, skl.tail.score
	if min < minScore {
		min = minScore
	}
	if max > maxScore {
		max = maxScore
	}

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].backward != nil && p.level[i].backward.score < min {
			p = p.level[i].backward
		}
	}

	p = p.level[0].backward
	for p != nil {
		if p.score > max {
			break
		}

		val = append(val, p.member, p.score)
		p = p.level[0].backward
	}

	return
}

// RevScoreRange returns all the elements whose score is between min and max.
// The elements are consideres to be ordered from high to low scores.
func (skl *SkipList) RevScoreRange(min, max float64) (val []interface{}) {
	minScore, maxScore := skl.head.level[0].backward.score, skl.tail.score
	if min < minScore {
		min = minScore
	}
	if max > maxScore {
		max = maxScore
	}

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].backward != nil && p.level[i].backward.score <= max {
			p = p.level[i].backward
		}
	}

	for p != nil {
		if p.score < min {
			break
		}

		val = append(val, p.member, p.score)
		p = p.forward
	}

	return
}

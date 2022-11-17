package index

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

// @Author KHighness
// @Update 2022-11-15

const (
	// maxLevel is the max level of the skip list.
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

// handleEle iterates the skip list node, ends when the returned value is false.
type handleEle func(e *Element) bool

// Node defines the structure of skip list node.
type Node struct {
	// next[0] is the first level, saves the original data arranged in an orderly manner.
	next []*Element
}

// Element defines the structure of element which is the data stored.
type Element struct {
	Node
	key   []byte
	value interface{}
}

// Key returns the key of element.
func (e *Element) Key() []byte {
	return e.key
}

// Value returns the value of element.
func (e *Element) Value() interface{} {
	return e.value
}

// SetValue sets the value for element.
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

// Next returns element's next node in the first level.
func (e *Element) Next() *Element {
	return e.next[0]
}

// SkipList defines the structure of skip list.
// For a skip list whose max level is 7, structure likes the following example:
// |l6| ---------> |23| -------------------------> |58| ---------------------------------> |nil|
// |l5| ---------> |23| -----------------> |46| -> |58| ---------------------------------> |nil|
// |l4| ---------> |23| ---------> |43| -> |46| -> |58| ---------------------------------> |nil|
// |l3| ---------> |23| -> |35| -> |43| -> |46| -> |58| -> |60| -------------------------> |nil|
// |l2| -> |12| -> |23| -> |35| -> |43| -> |46| -> |58| -> |60| -> |61| -----------------> |nil|
// |l1| -> |12| -> |23| -> |35| -> |43| -> |46| -> |58| -> |60| -> |61| -> |62| -> |65| -> |nil|
// |l0| -> |12| -> |23| -> |35| -> |43| -> |46| -> |58| -> |60| -> |61| -> |62| -> |65| -> |nil|
type SkipList struct {
	Node
	maxLevel      int
	len           int
	randSource    rand.Source
	probability   float64
	probTable     []float64
	prevNodeCache []*Node
}

// NewSkipList creates a new skip list.
func NewSkipList() *SkipList {
	return &SkipList{
		Node:          Node{next: make([]*Element, maxLevel)},
		maxLevel:      maxLevel,
		randSource:    rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:   probability,
		probTable:     probabilityTable(probability, maxLevel),
		prevNodeCache: make([]*Node, maxLevel),
	}
}

// Front returns the first element of skip list.
// You can traverse the data in the following way:
//   e := skl.Front()
//   for p := e; p != nil; p = p.Next() {
//     // do something with p
//   }
func (skl *SkipList) Front() *Element {
	return skl.next[0]
}

// Size returns the count of element in the skip list.
func (skl *SkipList) Size() int {
	return skl.len
}

// Put puts an element into skip list, replaces the value if key already exists.
func (skl *SkipList) Put(key []byte, value interface{}) *Element {
	var elem *Element
	prev := skl.previousNodes(key)

	if elem = prev[0].next[0]; elem != nil && bytes.Compare(elem.key, key) <= 0 {
		elem.value = value
		return elem
	}

	elem = &Element{
		Node:  Node{next: make([]*Element, skl.randomLevel())},
		key:   key,
		value: value,
	}
	for i := range elem.next {
		elem.next[i] = prev[i].next[i]
		prev[i].next[i] = elem
	}

	skl.len++
	return elem
}

// Get searches value by the key, returns nil if not found.
func (skl *SkipList) Get(key []byte) *Element {
	var prev = &skl.Node
	var elem *Element

	for i := skl.maxLevel - 1; i >= 0; i-- { // from top to bottom
		elem = prev.next[i]

		for elem != nil && bytes.Compare(key, elem.key) > 0 { // from left to right
			prev = &elem.Node
			elem = elem.next[i]
		}
	}

	if elem != nil && bytes.Compare(elem.key, key) <= 0 {
		return elem
	}
	return nil
}

// Exist checks if the key exists in the skip list.
func (skl *SkipList) Exist(key []byte) bool {
	return skl.Get(key) != nil
}

// Remove removes element in the skip list by the specified key.
func (skl *SkipList) Remove(key []byte) *Element {
	prev := skl.previousNodes(key)

	if elem := prev[0].next[0]; elem != nil && bytes.Compare(elem.key, key) <= 0 {
		for k, v := range elem.next {
			prev[k].next[k] = v
		}

		skl.len--
		return elem
	}
	return nil
}

// Foreach iterates all elements in the skip list.
func (skl *SkipList) Foreach(fun handleEle) {
	for p := skl.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// FindPrefix finds the first element whose key matches the specified prefix string.
func (skl *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &skl.Node
	var elem *Element

	for i := skl.maxLevel - 1; i >= 0; i-- {
		elem = prev.next[i]

		for elem != nil && bytes.Compare(prefix, elem.key) > 0 {
			prev = &elem.Node
			elem = elem.next[i]
		}
	}

	if elem == nil {
		elem = skl.Front()
	}

	return elem
}

// previousNodes returns the previous node for the specified key,
func (skl *SkipList) previousNodes(key []byte) []*Node {
	var prev = &skl.Node
	var elem *Element

	forward := skl.prevNodeCache
	for i := skl.maxLevel - 1; i >= 0; i-- {
		elem = prev.next[i]

		for elem != nil && bytes.Compare(key, elem.key) > 0 {
			prev = &elem.Node
			elem = elem.next[i]
		}

		forward[i] = prev
	}

	return forward
}

// randomLevel generates a random level for a new element.
func (skl *SkipList) randomLevel() (level int) {
	r := float64(skl.randSource.Int63()) / (1 << 63)

	level = 1
	for level < skl.maxLevel && r < skl.probTable[level] {
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}

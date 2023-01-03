package art

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

// @Author KHighness
// @Update 2022-12-25

// AdaptiveRadixTree wrapper goart.Tree.
type AdaptiveRadixTree struct {
	tree goart.Tree
}

// NewART creates a adaptive radix tree.
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
	}
}

// Put puts key-value pair.
// Both parameter key and value can be nil.
func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldVal interface{}, updated bool) {
	return art.tree.Insert(key, value)
}

// Get gets value of the key.
// Parameter key can be nil.
func (art *AdaptiveRadixTree) Get(key []byte) interface{} {
	value, _ := art.tree.Search(key)
	return value
}

// Get deletes key-value pair according to the key.
// Parameter key can be nil.
func (art AdaptiveRadixTree) Delete(key []byte) (val interface{}, updated bool) {
	return art.tree.Delete(key)
}

// Iterator returns the iterator of the tree.
func (art *AdaptiveRadixTree) Iterator() goart.Iterator {
	return art.tree.Iterator()
}

// PrefixScan scans the specified number of values according to the specified prefix.
func (art *AdaptiveRadixTree) PrefixScan(prefix []byte, count int) (keys [][]byte) {
	cb := func(node goart.Node) bool {
		if node.Kind() != goart.Leaf {
			return true
		}
		if count <= 0 {
			return false
		}
		keys = append(keys, node.Key())
		count--
		return true
	}

	if len(prefix) == 0 {
		art.tree.ForEach(cb)
	} else {
		art.tree.ForEachPrefix(prefix, cb)
	}
	return
}

// Size returns the count of the elements in tree.
func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}

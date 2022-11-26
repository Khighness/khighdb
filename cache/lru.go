package cache

import (
	"container/list"
	"sync"
)

// @Author KHighness
// @Update 2022-11-26

// LruCache defines the structure of least recently used cache.
type LruCache struct {
	capacity  int
	cacheMap  map[string]*list.Element
	cacheList *list.List
	mu        sync.Mutex
}

type lruItem struct {
	key   string
	value []byte
}

// NewLRUCache creates a new LRU cache.
func NewLRUCache(capacity int) *LruCache {
	lru := &LruCache{}
	if capacity > 0 {
		lru.capacity = capacity
		lru.cacheMap = make(map[string]*list.Element)
		lru.cacheList = list.New()
	}
	return lru
}

// Get gets the value by a key.
func (lru *LruCache) Get(key []byte) ([]byte, bool) {
	if lru.capacity <= 0 || len(lru.cacheMap) <= 0 {
		return nil, false
	}
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.get(string(key))
}

func (lru *LruCache) get(key string) ([]byte, bool) {
	if elem, ok := lru.cacheMap[key]; ok {
		lru.cacheList.MoveToFront(elem)
		item := elem.Value.(*lruItem)
		return item.value, true
	}
	return nil, false
}

// Set add a key-value pair to cache, the value will updated it the key already exists.
func (lru *LruCache) Set(key, value []byte) {
	if lru.capacity <= 0 || key == nil {
		return
	}
	lru.mu.Lock()
	defer lru.mu.Unlock()
	lru.set(string(key), value)
}

func (lru *LruCache) set(key string, value []byte) {
	elem, ok := lru.cacheMap[key]
	if ok {
		item := lru.cacheMap[key].Value.(*lruItem)
		item.value = value
		lru.cacheList.MoveToFront(elem)
	} else {
		elem = lru.cacheList.PushFront(&lruItem{key: key, value: value})
		lru.cacheMap[key] = elem

		if lru.cacheList.Len() > lru.capacity {
			lru.removeOldest()
		}
	}
}

func (lru *LruCache) removeOldest() {
	elem := lru.cacheList.Back()
	lru.cacheList.Remove(elem)
	item := elem.Value.(*lruItem)
	delete(lru.cacheMap, item.key)
}

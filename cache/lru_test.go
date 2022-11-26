package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// @Author KHighness
// @Update 2022-11-26

func TestNewLRUCache(t *testing.T) {
	cache := NewLRUCache(64)
	assert.NotNil(t, cache)
}

func TestLruCache_Set(t *testing.T) {
	cache := NewLRUCache(3)
	cache.Set([]byte("k1"), []byte("v1"))
}

func TestLruCache_Get(t *testing.T) {
	cache := NewLRUCache(3)
	cache.Set([]byte("k1"), []byte("v1"))
	v1, ok := cache.Get([]byte("k1"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("v1"), v1)

	cache.Set([]byte("k2"), []byte("v2"))
	cache.Set([]byte("k3"), []byte("v3"))
	cache.Set([]byte("k4"), []byte("v4"))
	v1, ok = cache.Get([]byte("k1"))
	assert.Equal(t, false, ok)
}

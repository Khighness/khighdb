package khighdb

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/Khighness/khighdb/storage"
	"github.com/Khighness/khighdb/util"
)

// @Author KHighness
// @Update 2023-01-01

// Set sets key to hold the string value.
// If key already holds a value, it will be overwritten.
// Any previous time to live associated with the key is
// discarded o successful set operation.
func (db *KhighDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &storage.LogEntry{Key: key, Value: value}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	return db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
}

// Get gets the value of the key.
// If the key does not exist, ErrKeyNotFound is returned.
func (db *KhighDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

// MSet sets key-value pairs in batches.
// Parameter should be like [key, value, key, value...]
func (db *KhighDB) MSet(args ...[]byte) error {
	if len(args) == 0 || len(args)%2 != 0 {
		return ErrInvalidNumberOfArgs
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key, value := args[0], args[1]
		entry := &storage.LogEntry{Key: key, Value: value}
		pos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
		if err != nil {
			return err
		}
	}
	return nil
}

// MGet gets the values of all specified keys.
// If just a single key does not exist, ErrKeyNotFound is returned.
func (db *KhighDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrInvalidNumberOfArgs
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// GetRange returns the substring of the string value stored at key,
// determined by the offset start and end.
func (db *KhighDB) GetRange(key []byte, start, end int) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return []byte{}, nil
	}
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}

	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > len(val)-1 || start > end {
		return []byte{}, nil
	}
	return val[start : end+1], nil
}

// GetDel gets the value of the key and deletes the key.
func (db *KhighDB) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	entry := &storage.LogEntry{Key: key, Type: storage.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return nil, err
	}
	_, size := storage.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].nodeChan <- node:
	default:
		zap.L().Warn("failed to send node to discard channel")
	}
	return val, nil
}

// Delete deletes key-value pair corresponding to the given key.
func (db *KhighDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &storage.LogEntry{Key: key, Type: storage.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	val, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(val, updated, String)
	_, size := storage.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].nodeChan <- node:
	default:
		zap.L().Warn("failed to send node to discard channel")
	}
	return nil
}

// SetEX sets key to hold the string value with expiration time.
func (db *KhighDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	expiredAt := time.Now().Add(duration).Unix()
	entry := &storage.LogEntry{Key: key, Value: value, ExpiredAt: expiredAt}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	return db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
}

// SetNX sets key to hold the string value if the key is not exist.
// If the key already exists, nil is return,
func (db *KhighDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if val != nil {
		return nil
	}

	entry := &storage.LogEntry{Key: key, Value: value}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	return db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
}

// MSetNX executes SetNX in batches.
// If just a single key already exist, all the SetNX
// operations will not be performed and nil will returned.
func (db *KhighDB) MSetNX(args ...[]byte) error {
	if len(args) == 0 || len(args)%2 != 0 {
		return ErrInvalidNumberOfArgs
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil {
			return err
		}
		if val != nil {
			return nil
		}
	}

	// Filter the duplicate keys.
	var addedKeys = make(map[uint64]struct{})
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		h := util.MemHash(key)
		if _, ok := addedKeys[h]; ok {
			continue
		}
		entry := &storage.LogEntry{Key: key, Value: value}
		pos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
		if err != nil {
			return err
		}
		addedKeys[h] = struct{}{}
	}
	return nil
}

// Append appends the value at the end of the old value if the key already exists.
// This function executes the same operation as Set if ytje key does not exist.
func (db *KhighDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	if oldVal != nil {
		value = append(oldVal, value...)
	}
	entry := &storage.LogEntry{Key: key, Value: value}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
}

// Incr increments the value stored at key.
// If the key does not exist, the value will be set to 0 before performing this operation.
// It returns ErrInvalidValueType if the value type is not integer type.
// Also, it returns ErrIntegerOverflow if the value exceeds after incrementing the value.
func (db *KhighDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.deltaBy(key, 1)
}

// IncrBy increases the value for the specified delta stored at key.
// If the key does not exist, the value will be set to 0 before performing this operation.
// It returns ErrInvalidValueType if the value type is not integer type.
// Also, it returns ErrIntegerOverflow if the value exceeds after incrementing the value.
func (db *KhighDB) IncrBy(key []byte, delta int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.deltaBy(key, delta)
}

// Decr decrements the value for the specified delta stored at key.
// If the key does not exist, the value will be set to 0 before performing this operation.
// It returns ErrInvalidValueType if the value type is not integer type.
// Also, it returns ErrIntegerOverflow if the value exceeds after decrementing the value.
func (db *KhighDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.deltaBy(key, 1)
}

// IncrBy decreases the value stored at key.
// If the key does not exist, the value will be set to 0 before performing this operation.
// It returns ErrInvalidValueType if the value type is not integer type.
// Also, it returns ErrIntegerOverflow if the value exceeds after decreasing the value.
func (db *KhighDB) DecrBy(key []byte, delta int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.deltaBy(key, -delta)
}

// deltaBy updates the integer value corresponding to key.
// This function should be invoked with write lock.
// It returns the updated value. It returns 0 if any error occurs.
func (db *KhighDB) deltaBy(key []byte, delta int64) (int64, error) {
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrInvalidValueType
	}

	if (valInt64 < 0 && delta < 0 && delta < math.MinInt64-valInt64) ||
		(valInt64 > 0 && delta > 0 && delta > math.MaxInt64-valInt64) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += delta
	val = []byte(strconv.FormatInt(valInt64, 10))
	entry := &storage.LogEntry{Key: key, Value: val}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, entry, pos, true, String)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// StrLen returns the length of string value stored at key.
// If the keys does not exist, o is return.
func (db *KhighDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}
	return len(val)
}

// Count returns the total number of keys of String.
func (db *KhighDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return 0
	}
	return db.strIndex.idxTree.Size()
}

// Scan iterates over all keys of type String and finds its value.
// Parameter prefix will matches key's prefix, and pattern is a regular expression that
// also matches the key. Parameter count limits the number of keys, a empty slice will
// will be returned is count is not a positive number.
// The returned values will be a mixed data of keys and values, like [key, value, key, value ...].
func (db *KhighDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	var reg *regexp.Regexp
	var err error
	if pattern != "" {
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	db.strIndex.mu.RLock()
	db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return nil, nil
	}
	keys := db.strIndex.idxTree.PrefixScan(prefix, count)
	if len(keys) == 0 {
		return nil, nil
	}

	var result [][]byte
	for _, key := range keys {
		if reg != nil && !reg.Match(key) {
			continue
		}
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		if !errors.Is(err, ErrKeyNotFound) {
			result = append(result, key, val)
		}
	}
	return result, nil
}

// Expire sets the expiration time for the given key.
func (db *KhighDB) Expire(key []byte, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()
	return db.SetEX(key, val, duration)
}

// TTL gets time to live for the given key.
func (db *KhighDB) TTL(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	node, err := db.getIndexNode(db.strIndex.idxTree, key)
	if err != nil {
		return 0, err
	}
	var ttl int64
	if node.expiredAt != 0 {
		ttl = node.expiredAt - time.Now().Unix()
	}
	return ttl, nil
}

// Persist removes the expiration time for the given key.
func (db *KhighDB) Persist(key []byte) error {
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()
	return db.Set(key, val)
}

// GetStrKeys returns all the stored keys of type String.
func (db *KhighDB) GetStrKeys() ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return nil, nil
	}
	var keys [][]byte
	iterator := db.strIndex.idxTree.Iterator()
	ts := time.Now().Unix()
	for iterator.HasNext() {
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		idxNode := node.Value().(*indexNode)
		if idxNode == nil {
			continue
		}
		if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
			continue
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

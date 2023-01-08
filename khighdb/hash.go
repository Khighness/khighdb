package khighdb

import (
	"bytes"
	"errors"
	"github.com/Khighness/khighdb/data/art"
	"github.com/Khighness/khighdb/storage"
	"github.com/Khighness/khighdb/util"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"time"
)

// @Author KHighness
// @Update 2023-01-07

// HSet sets filed in the hash stored at key to value.
// If the key does not exist, a new key holding a hash is created.
// If the filed already exists in the hash, it is overwritten.
// If you want to set multiple filed-value pair, parameter args be
// like ['filed', 'value', 'field', 'value'...].
func (db *KhighDB) HSet(key []byte, args ...[]byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if len(args) == 0 || len(args)&1 == 1 {
		return ErrInvalidNumberOfArgs
	}
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	for i := 0; i < len(args); i += 2 {
		filed, value := args[i], args[i+1]
		hashKey := db.encodeKey(key, filed)
		ent := &storage.LogEntry{Key: hashKey, Value: value}
		pos, err := db.writeLogEntry(ent, Hash)
		if err != nil {
			return err
		}

		entry := &storage.LogEntry{Key: filed, Value: value}
		_, size := storage.EncodeEntry(entry)
		pos.entrySize = size

		err = db.updateIndexTree(idxTree, entry, pos, true, Hash)
		if err != nil {
			return err
		}
	}
	return nil
}

// HSetNX sets the given value obly if the field does not exist.
// If the key does not exist, a new hash is created.
// If the field already exists, HSetNX does not have side effect.
func (db *KhighDB) HSetNX(key, field, value []byte) (bool, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return false, err
	}
	if val != nil {
		return false, nil
	}

	hashKey := db.encodeKey(key, field)
	ent := &storage.LogEntry{Key: hashKey, Value: value}
	pos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return false, err
	}

	entry := &storage.LogEntry{Key: field, Value: value}
	_, size := storage.EncodeEntry(entry)
	pos.entrySize = size
	err = db.updateIndexTree(idxTree, entry, pos, true, Hash)
	if err != nil {
		return false, err
	}
	return true, nil
}

// HGet returns the value associated with field in the hash stored at key.
func (db *KhighDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if errors.Is(err, ErrKeyNotFound) {
		return nil, nil
	}
	return val, nil
}

// HMGet returns the values associated with the specified fields in the hash stored
// at key. For every field that does not exist in the hash, nil is returned.
// If the key does not exist, a list of nil values is returned.
func (db *KhighDB) HMGet(key []byte, fields ...[]byte) (vals [][]byte, err error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	length := len(fields)

	if db.hashIndex.trees[string(key)] == nil {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
	} else {
		idxTree := db.hashIndex.trees[string(key)]
		for _, field := range fields {
			val, err := db.getVal(idxTree, field, Hash)
			if err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					vals = append(vals, nil)
				} else {
					return nil, err
				}
			} else {
				vals = append(vals, val)
			}
		}
	}
	return
}

// HDel removes the specified fields from the hash stored at key
// and returns the number of the fields removed successfully. .
func (db *KhighDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0, nil
	}
	idxTree := db.hashIndex.trees[string(key)]

	var count int
	for _, field := range fields {
		hashKey := db.encodeKey(key, field)
		entry := &storage.LogEntry{Key: hashKey, Type: storage.TypeDelete}
		pos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return 0, err
		}

		val, updated := idxTree.Delete(field)
		if updated {
			count++
		}
		db.sendDiscard(val, updated, Hash)

		_, size := storage.EncodeEntry(entry)
		node := &indexNode{fid: pos.fid, entrySize: size}
		// The deleted entry itself is also useless.
		select {
		case db.discards[Hash].nodeChan <- node:
		default:
			zap.L().Warn("failed to send node to discard channel")
		}
	}
	return count, nil
}

// HExists returns whether the field exists in the hash stored at key.
func (db *KhighDB) HExists(key, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return false, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil {
		return false, err
	}
	return val != nil, err
}

// HLen returns the number of fields contained in the hash stored at key.
func (db *KhighDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.hashIndex.trees[string(key)]
	return idxTree.Size()
}

// HKeys return all field names in the hash stored at key,
func (db *KhighDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var keys [][]byte
	idxTree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return keys, nil
	}
	iterator := idxTree.Iterator()
	for iterator.HasNext() {
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

// HKeys return all field values in the hash stored at key,
func (db *KhighDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var vals [][]byte
	idxTree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return vals, nil
	}
	iterator := idxTree.Iterator()
	for iterator.HasNext() {
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getVal(idxTree, node.Key(), Hash)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

// HGetAll returns all fields and values in the hash stored at key.
// The returned data likes ['field', 'value', 'field', 'value'...].
func (db *KhighDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return [][]byte{}, nil
	}

	var index int
	pairs := make([][]byte, idxTree.Size()*2)
	iterator := idxTree.Iterator()
	for iterator.HasNext() {
		node, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		field := node.Key()
		value, err := db.getVal(idxTree, field, Hash)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		pairs[index], pairs[index+1] = field, value
		index += 2
	}
	return pairs[:index], nil
}

// HStrLen returns the string length associated with field in the hash stored at key,
// If the key or the field do not exist, 0 is returned.
func (db *KhighDB) HStrLen(key, field []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if errors.Is(err, ErrKeyNotFound) {
		return 0
	}
	return len(val)
}

// HScan iterates over a specified key of type Hash and finds its fields and values.
// Parameter prefix will match field's prefix, and pattern is a regular expression that
// also matches the field. Parameter count limits the  number of keys, a nil slice will
// be returned if count is not a positive number,
// The returned data likes ['field', 'value', 'field', 'value'...].
func (db *KhighDB) HScan(key []byte, prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	fields := idxTree.PrefixScan(prefix, count)
	if len(fields) == 0 {
		return nil, nil
	}

	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	values := make([][]byte, 2*len(fields))
	var index int
	for _, field := range fields {
		if reg != nil && !reg.Match(field) {
			continue
		}
		val, err := db.getVal(idxTree, field, Hash)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		values[index], values[index+1] = field, val
		index += 2
	}
	return values, nil
}

// HIncrBy increases the number stored at field in the hash stored at key by delta.
// If the key does not exist, a new key holding a hash is created,
// If the filed does not exist, the value is set to 0 before performing this operation.
func (db *KhighDB) HIncrBy(key, field []byte, delta int64) (int64, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}

	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := util.StrToInt64(string(val))
	if err != nil {
		return 0, ErrInvalidValueType
	}

	if (delta < 0 && valInt64 < 0 && delta < (math.MinInt64-valInt64)) ||
		(delta > 0 && valInt64 > 0 && delta > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += delta
	val = []byte(strconv.FormatInt(valInt64, 10))

	hashKey := db.encodeKey(key, field)
	ent := &storage.LogEntry{Key: hashKey, Value: val}
	pos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return 0, err
	}

	entry := &storage.LogEntry{Key: field, Value: val}
	_, size := storage.EncodeEntry(ent)
	pos.entrySize = size
	err = db.updateIndexTree(idxTree, entry, pos, true, Hash)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// HRandField returns the fields from the hash value stored at key.
//  The count argument controls the returned data in following ways:
//  - count = 0: Return a random field.
//  - count > 0: Return an array of distinct fields.
//  - count < 0: The returned fields is allowed to return the same field multiple times.
func (db *KhighDB) HRandField(key []byte, count int, withValues bool) ([][]byte, error) {
	if count == 0 {
		return [][]byte{}, nil
	}
	var (
		values     [][]byte
		err        error
		pairLength = 1
	)
	if withValues {
		pairLength = 2
		values, err = db.HGetAll(key)
	} else {
		values, err = db.HKeys(key)
	}
	if err != nil {
		return [][]byte{}, err
	}
	if len(values) == 0 {
		return [][]byte{}, nil
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	pairCount := len(values) / pairLength

	if count > 0 {
		if count >= pairCount {
			return values, nil
		}
		var noDupValues = values
		diff := pairCount - count
		for i := 0; i < diff; i++ {
			rndIdx := rnd.Intn(len(noDupValues)/pairLength) * pairLength
			noDupValues = append(noDupValues[:rndIdx], noDupValues[rndIdx+pairLength:]...)
		}
		return noDupValues, nil
	}
	count = -count
	var dupValues [][]byte
	for i := 0; i < count; i++ {
		rndIdx := rnd.Intn(pairCount) * pairLength
		dupValues = append(dupValues, values[rndIdx:rndIdx+pairLength]...)
	}
	return dupValues, nil
}

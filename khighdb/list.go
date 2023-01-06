package khighdb

import (
	"encoding/binary"
	"errors"
	"go.uber.org/zap"
	"math"

	"github.com/Khighness/khighdb/data/art"
	"github.com/Khighness/khighdb/storage"
)

// @Author KHighness
// @Update 2022-12-28

// List‘s structure is as follows:
//	+---------+---------+---------+---------+---------+---------+-----------+
//	|    0    |    1    |   ...   | headSeq | tailSeq |   ...   | MaxUint32 |
//	+---------+---------+---------+---------+---------+---------+-----------+
//	| <----------------------------- LPush  |  RPush ---------------------> |
const initialListSeq = math.MaxInt32

// LPush inserts all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *KhighDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// LPushX inserts a specified values at the head of the list stored at key,
// only if key already exists and holds a list.
// In contrary to LPush, no operation will be performed and ErrKeyNotFound
// will be returned if the key does not exist.
func (db *KhighDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// RPush inserts all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *KhighDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// RPushX inserts a specified values at the head of the list stored at key,
// only if key already exists and holds a list.
// In contrary to LPush, no operation will be performed and ErrKeyNotFound
// will be returned if the key does not exist.
func (db *KhighDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first element of the list stored at key,
func (db *KhighDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}

// LPop removes and returns the last element of the list stored at key,
func (db *KhighDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// LIndex returns the element at index in the list stored at key.
// If the index is 0, it returns the first element.
// If the index a positive number, it returns the (index+1)-th element.
// If the index is a negative number, it returns the index-th element
// from the tail. Also, if the index is out of range, it returns nil.
func (db *KhighDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}
	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrIndexOutOfRange
	}

	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// encodeListKey encodes the key and the sequence into a byte slice.
func (db *KhighDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}

// decodeListKey decodes the byte slice into a key and a sequence.
func (db *KhighDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}

// listMeta returns the head sequence and the tail sequence corresponding to the key.
func (db *KhighDB) listMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	val, err := db.getVal(idxTree, key, List)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, 0, err
	}

	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

// saveListMeta saves the the meta information of the key to the list'index tree.
func (db *KhighDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	ent := &storage.LogEntry{Key: key, Value: buf, Type: storage.TypeListMeta}
	por, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(idxTree, ent, por, true, List)
	return err
}

// pushInternal inserts a value at the head or tail of the list stored at key.
// Parameter isLeft controls the insert position, if true the value will be
// inserted at the list's head, otherwise it will be inserted at the list's tail.
func (db *KhighDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}
	var seq = headSeq
	if !isLeft {
		seq = tailSeq
	}
	encKey := db.encodeListKey(key, seq)
	ent := &storage.LogEntry{Key: encKey, Value: val}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}

	if err = db.updateIndexTree(idxTree, ent, pos, true, List); err != nil {
		return err
	}

	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}
	err = db.saveListMeta(idxTree, key, headSeq, tailSeq)
	return err
}

// popInternal removes and returns the head or tail of the list stored at key.
// Parameter isLeft controls the remove position, if true the head of the list
// will be removed and returned, otherwise it will be the tail of the list will
// be removed and returned. Also, if the list is empty, it returns nil.
func (db *KhighDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	if tailSeq-headSeq-1 <= 0 {
		return nil, nil
	}

	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}

	ent := &storage.LogEntry{Key: encKey, Type: storage.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	oldVal, updated := idxTree.Delete(encKey)
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.saveListMeta(idxTree, key, headSeq, tailSeq); err != nil {
		return nil, err
	}

	db.sendDiscard(oldVal, updated, List)
	_, entrySize := storage.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].nodeChan <- idxNode:
	default:
		zap.L().Warn("failed to send node to discard channel")
	}
	if tailSeq-headSeq-1 == 0 {
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveListMeta(idxTree, key, headSeq, tailSeq)
		}
		delete(db.listIndex.trees, string(key))
	}
	return val, nil
}

// listSequence converts logic index to phisical sequence.
func (db *KhighDB) listSequence(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32

	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}

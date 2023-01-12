package khighdb

import (
	"github.com/Khighness/khighdb/util"
	"go.uber.org/zap"

	"github.com/Khighness/khighdb/data/art"
	"github.com/Khighness/khighdb/storage"
)

// @Author KHighness
// @Update 2023-01-10

// SAdd adds the specified members to the members to the set stored at key.
// Specified members which are already a member of this set are ignored.
// If the key does not exist, a new set is created before adding the specified members.
func (db *KhighDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(key)]
	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}
		if err := db.setIndex.murhash.Write(mem); err != nil {
			return err
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		ent := &storage.LogEntry{Key: key, Value: mem}
		pos, err := db.writeLogEntry(ent, Set)
		if err != nil {
			return err
		}
		entry := &storage.LogEntry{Key: sum, Value: mem}
		_, size := storage.EncodeEntry(entry)
		pos.entrySize = size
		if err := db.updateIndexTree(idxTree, entry, pos, true, Set); err != nil {
			return err
		}
	}
	return nil
}

// SPop removes and returns specified number of members from the set stored at key.
func (db *KhighDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.setIndex.trees[string(key)]

	var values [][]byte
	iterator := idxTree.Iterator()
	for iterator.HasNext() && count > 0 {
		count--
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	for _, val := range values {
		if err := db.sRemInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SRem removes the specified members from the set stored at key.
func (db *KhighDB) SRem(key []byte, member ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil
	}
	for _, mem := range member {
		if err := db.sRemInternal(key, mem); err != nil {
			return err
		}
	}
	return nil
}

// SIsMember checks if the given member is a member of the set stored at key.
func (db *KhighDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.setIndex.trees[string(key)] == nil {
		return false
	}
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return false
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()
	node := idxTree.Get(sum)
	return node != nil
}

// SMembers returns all the members of the set value stored at key.
func (db *KhighDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.sMembers(key)
}

// SCard returns the set cardinality (number of elements) stored at key.
func (db *KhighDB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	idxTree := db.setIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}
	return idxTree.Size()
}

// SDiff returns the members of the set difference between the
// first set and all the successive sets.
func (db *KhighDB) SDiff(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, ErrInvalidNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	firstSet, err := db.sMembers(keys[0])
	if err != nil {
		return nil, err
	}
	set := make(map[uint64]struct{})
	for _, key := range keys[1:] {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			h := util.MemHash(member)
			if _, ok := set[h]; !ok {
				set[h] = struct{}{}
			}
		}
	}
	if len(set) == 0 {
		return firstSet, nil
	}
	res := make([][]byte, 0)
	for _, member := range firstSet {
		h := util.MemHash(member)
		if _, ok := set[h]; !ok {
			res = append(res, member)
		}
	}
	return res, nil
}

// SDiffStore is equal to SDiff, the result is stored in
// first param instead of being returned.
// It returns the cardinality of the result normally.
// Also, it returns -1 if any error occurs.
func (db *KhighDB) SDiffStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	diff, err := db.SDiff(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err = db.sStore(destination, diff); err != nil {
		return -1, err
	}
	return db.SCard(destination), nil
}

// SUnion returns the members of the set resulting from
// the union of all the given sets.
func (db *KhighDB) SUnion(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrInvalidNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	set := make(map[uint64]struct{})
	unionSet := make([][]byte, 0)
	for _, key := range keys {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			h := util.MemHash(member)
			if _, ok := set[h]; !ok {
				set[h] = struct{}{}
				unionSet = append(unionSet, member)
			}
		}
	}
	return unionSet, nil
}

// SDiffStore is equal to SUnion, the result is stored in
// first param instead of being returned.
// It returns the cardinality of the result normally.
// Also, it returns -1 if any error occurs.
func (db *KhighDB) SUnionStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	union, err := db.SUnion(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, union); err != nil {
		return -1, err
	}
	return db.SCard(destination), nil
}

// SInter returns the members of the set resulting from
// the inter if all the given sets.
func (db *KhighDB) SInter(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrInvalidNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	num := len(keys)
	set := make(map[uint64]int)
	interSet := make([][]byte, 0)
	for _, key := range keys {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, val := range members {
			h := util.MemHash(val)
			set[h]++
			if set[h] == num {
				interSet = append(interSet, val)
			}
		}
	}
	return interSet, nil
}

// SInterStore is equal to SInter, the result is stored in
// first param instead of being returned.
// It returns the cardinality of the result normally.
// Also, it returns -1 if any error occurs.
func (db *KhighDB) SInterStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	inter, err := db.SInter(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, inter); err != nil {
		return -1, err
	}
	return db.SCard(destination), nil
}

// sRemInternal removes a member from the set stored at key.
func (db *KhighDB) sRemInternal(key []byte, member []byte) error {
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	val, updated := idxTree.Delete(sum)
	if !updated {
		return nil
	}
	entry := &storage.LogEntry{Key: key, Value: sum, Type: storage.TypeDelete}
	pos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return err
	}

	db.sendDiscard(val, updated, Set)
	_, size := storage.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[Set].nodeChan <- node:
	default:
		zap.L().Warn("Failed to send node to discard channel")
	}
	return nil
}

// sMembers returns all members of the set stored at key.
func (db *KhighDB) sMembers(key []byte) ([][]byte, error) {
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}

	var values [][]byte
	idxTree := db.setIndex.trees[string(key)]
	iterator := idxTree.Iterator()
	for iterator.HasNext() {
		node, _ := iterator.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// sStore stores vals in the set the destination points to.
func (db *KhighDB) sStore(destination []byte, vals [][]byte) error {
	for _, val := range vals {
		if isMember := db.SIsMember(destination, val); !isMember {
			if err := db.SAdd(destination, val); err != nil {
				return err
			}
		}
	}
	return nil
}

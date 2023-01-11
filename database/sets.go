package khighdb

import (
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

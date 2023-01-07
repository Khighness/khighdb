package khighdb

import (
	"github.com/Khighness/khighdb/data/art"
	"github.com/Khighness/khighdb/storage"
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
		entry := &storage.LogEntry{Key: hashKey, Value: value}
		pos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return err
		}

		ent := &storage.LogEntry{Key: filed, Value: value}
		_, size := storage.EncodeEntry(entry)
		pos.entrySize = size
		err = db.updateIndexTree(idxTree, ent, pos, true, Hash)
		if err != nil {
			return err
		}
	}
	return nil
}

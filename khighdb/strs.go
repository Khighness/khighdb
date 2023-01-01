package khighdb

import "github.com/Khighness/khighdb/storage"

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
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return err
}

// Get gets the value of the key.
// If the key does not exist, ErrKeyNotFound will be returned.
func (db *KhighDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

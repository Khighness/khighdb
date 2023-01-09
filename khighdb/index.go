package khighdb

import (
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/Khighness/khighdb/data/art"
	"github.com/Khighness/khighdb/storage"
	"github.com/Khighness/khighdb/util"
)

// @Author KHighness
// @Update 2022-12-27

// DataType defines the data structure type.
type DataType = int8

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *KhighDB) buildIndex(dataType DataType, ent *storage.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildSetsIndex(ent, pos)
	case List:
		db.buildListIndex(ent, pos)
	case Hash:
		db.buildHashIndex(ent, pos)
	case Set:
		db.buildSetsIndex(ent, pos)
	case ZSet:
		db.buildZSetIndex(ent, pos)
	}
}

func (db *KhighDB) buildStrsIndex(ent *storage.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == storage.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}

	_, size := storage.EncodeEntry(ent)
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	if db.openKeyValueMemMode() {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *KhighDB) buildListIndex(ent *storage.LogEntry, pos *valuePos) {
	var listKey = ent.Key
	if ent.Type != storage.TypeListMeta {
		listKey, _ = db.decodeListKey(ent.Key)
	}
	if db.listIndex.trees[string(listKey)] == nil {
		db.listIndex.trees[string(listKey)] = art.NewART()
	}
	idxTree := db.listIndex.trees[string(listKey)]

	if ent.Type == storage.TypeDelete {
		idxTree.Delete(ent.Key)
		return
	}
	_, size := storage.EncodeEntry(ent)
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	if db.openKeyValueMemMode() {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxTree)
}

func (db *KhighDB) buildHashIndex(ent *storage.LogEntry, pos *valuePos) {
	key, field := db.decodeKey(ent.Key)
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	if ent.Type == storage.TypeDelete {
		idxTree.Delete(field)
		return
	}

	_, size := storage.EncodeEntry(ent)
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	if db.openKeyValueMemMode() {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(field, idxNode)
}

func (db *KhighDB) buildSetsIndex(ent *storage.LogEntry, pos *valuePos) {
	if db.setIndex.trees[string(ent.Key)] == nil {
		db.setIndex.trees[string(ent.Key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(ent.Key)]

	if ent.Type == storage.TypeDelete {
		idxTree.Delete(ent.Value)
		return
	}

	if err := db.setIndex.murhash.Write(ent.Value); err != nil {
		zap.L().Error("Failed to write murmur hash", zap.Error(err))
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	_, size := storage.EncodeEntry(ent)
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	if db.openKeyValueMemMode() {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(sum, idxTree)
}

func (db *KhighDB) buildZSetIndex(ent *storage.LogEntry, pos *valuePos) {
	if ent.Type == storage.TypeDelete {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		if db.zsetIndex.trees[string(ent.Key)] != nil {
			db.zsetIndex.trees[string(ent.Key)].Delete(ent.Value)
		}
		return
	}

	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))
	if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
		zap.L().Fatal("Failed to write murmur hash", zap.Error(err))
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	idxTree := db.zsetIndex.trees[string(key)]
	if idxTree == nil {
		idxTree = art.NewART()
		db.zsetIndex.trees[string(key)] = idxTree
	}

	_, size := storage.EncodeEntry(ent)
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	if db.openKeyValueMemMode() {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	idxTree.Put(sum, idxTree)
}

func (db *KhighDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *storage.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				zap.L().Fatal("Log file is nil, failed to open db")
			}

			var offset int64
			for {
				entry, entrySize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == storage.ErrEndOfEntry {
						break
					}
					zap.L().Fatal("Read log entry from file err, failed to open db")
				}
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += entrySize
			}
			// Set latest log file's Write
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go iterateAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}

func (db *KhighDB) updateIndexTree(idxTree *art.AdaptiveRadixTree, ent *storage.LogEntry,
	pos *valuePos, sendDiscard bool, dataType DataType) error {

	var size = pos.entrySize
	if dataType == String || dataType == List {
		_, size = storage.EncodeEntry(ent)
	}
	idxNode := &indexNode{
		fid:       pos.fid,
		offset:    pos.offset,
		entrySize: size,
	}
	if db.openKeyValueMemMode() {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}

	oldVal, updated := idxTree.Put(ent.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated, dataType)
	}
	return nil
}

func (db *KhighDB) getIndexNode(idxTree *art.AdaptiveRadixTree, key []byte) (*indexNode, error) {
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}
	return idxNode, nil
}

func (db *KhighDB) getVal(idxTree *art.AdaptiveRadixTree, key []byte, dataType DataType) ([]byte, error) {
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	nano := time.Now().UnixNano()
	if idxNode.expiredAt != 0 && idxNode.expiredAt < nano {
		return nil, ErrKeyNotFound
	}

	// In KeyValueMemMode, the value is stored in memory.
	// So get the value from the index info.
	if db.openKeyValueMemMode() && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// In KeyOnlyMemMode, the value is stored in disk.
	// So get the value from log file at the offset.
	logFile := db.getActiveLogFile(dataType)
	if logFile.Fid != idxNode.fid {
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}

	entry, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	if entry.Type == storage.TypeDelete || (entry.ExpiredAt != 0 && entry.ExpiredAt < nano) {
		return nil, ErrKeyNotFound
	}
	return entry.Value, nil
}

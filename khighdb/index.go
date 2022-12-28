package khighdb

import (
	"github.com/khighness/khighdb/data/art"
	"github.com/khighness/khighdb/storage"
	"time"
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
	if db.options.IndexMode == KeyValueMemMode {
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
	if db.options.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxTree)
}

func (db *KhighDB) buildHashIndex(ent *storage.LogEntry, pos *valuePos) {

}

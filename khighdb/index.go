package khighdb

import "github.com/khighness/khighdb/storage"

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

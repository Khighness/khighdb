package index

import "github.com/khighness/khighdb/store"

// @Author KHighness
// @Update 2022-11-15

// Indexer records the information of data index, stored in the skip list.
type Indexer struct {
	Meta   store.Meta // metadata info.
	FileId uint32     // the file id of storing the data.
	Offset int64      // entry data query start position.
}

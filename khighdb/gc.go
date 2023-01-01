package khighdb

import (
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/khighness/khighdb/storage"
)

// @Author KHighness
// @Update 2022-12-31

// handleLogFileGC starts a ticker to execute gc periodically.
func (db *KhighDB) handleLogFileGC() {
	gcInternal := db.options.LogFileGCInternal
	if gcInternal <= 0 {
		return
	}

	quitFlag := make(chan os.Signal, 1)
	signal.Notify(quitFlag, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ticker := time.NewTicker(gcInternal)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			zap.L().Info("execute log file gc periodically", zap.Any("internal", gcInternal))
			if atomic.LoadInt32(&db.gcState) > 0 {
				zap.S().Warn("log file gc is running, skip it")
				break
			}
			for dataType := String; dataType < logFileTypeNum; dataType++ {
				go func(dataType DataType) {
					err := db.doRunGC(dataType, -1, db.options.LogFileGCRatio)
					if err != nil {
						zap.L().Error("log file gc err", zap.Int8("dataType", dataType), zap.Error(err))
					}
				}(dataType)
			}
		case <-quitFlag:
			return
		}
	}
}

// doRunGC checks if the specified archived log file's the ratio of garbage (delete entries)
// exceeds the specified ratio and then executes gc operation which is transferring the log
// entries in the archived log file to the active log file and deleting the archived log file.
func (db *KhighDB) doRunGC(dataType DataType, archivedLogFileId int, gcRatio float64) error {
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(fid uint32, offset int64, ent *storage.LogEntry) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()

		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, String)
			if err != nil {
				return err
			}
			// update index
			if err = db.updateIndexTree(db.strIndex.idxTree, ent, valuePos, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteList := func(fid uint32, offset int64, ent *storage.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()
		var listKey = ent.Key
		if ent.Type != storage.TypeListMeta {
			listKey, _ = db.decodeListKey(ent.Key)
		}
		if db.listIndex.trees[string(listKey)] == nil {
			return nil
		}
		idxTree := db.listIndex.trees[string(listKey)]
		indexVal := idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, ent, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(fid uint32, offset int64, ent *storage.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()
		key, field := db.decodeKey(ent.Key)
		if db.hashIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.hashIndex.trees[string(key)]
		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {

		}

		return nil
	}

	maybaRewriteSets := func(fid uint32, offset int64, ent *storage.LogEntry) error {
		return nil
	}

	maybaRewriteZSet := func(fid uint32, offset int64, ent *storage.LogEntry) error {
		return nil
	}

	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil
	}
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}

	// Find the archived log files which need to garbage collection.
	ccl, err := db.discards[dataType].getCCL(activeLogFile.Fid, gcRatio)
	if err != nil {
		return err
	}

	for _, fid := range ccl {
		if archivedLogFileId >= 0 && uint32(archivedLogFileId) != fid {
			continue
		}
		archivedLogFile := db.getArchivedLogFile(dataType, fid)
		if archivedLogFile == nil {
			continue
		}

		zap.L().Info("archived log file gc starts", zap.Uint32("fid", fid))
		// Transfer the log entries in archived log file to active log file.
		var offset int64
		for {
			ent, size, err := activeLogFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == storage.ErrEndOfEntry {
					break
				}
				return err
			}

			var rewriteOffset = offset
			offset += size
			if ent.Type == storage.TypeDelete {
				continue
			}
			ts := time.Now().Unix()
			if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
				continue
			}
			var rewriteErr error
			switch dataType {
			case String:
				rewriteErr = maybeRewriteStrs(archivedLogFile.Fid, rewriteOffset, ent)
			case List:
				rewriteErr = maybeRewriteList(archivedLogFile.Fid, rewriteOffset, ent)
			case Hash:
				rewriteErr = maybeRewriteHash(archivedLogFile.Fid, rewriteOffset, ent)
			case Set:
				rewriteErr = maybaRewriteSets(archivedLogFile.Fid, rewriteOffset, ent)
			case ZSet:
				rewriteErr = maybaRewriteZSet(archivedLogFile.Fid, rewriteOffset, ent)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}

		// Delete the older archived log file.
		db.mu.Lock()
		delete(db.archivedLogFiles[dataType], fid)
		if err = archivedLogFile.Delete(); err != nil {
			zap.L().Warn("failed to delete archived log file", zap.Error(err))
		}
		db.mu.Unlock()
		db.discards[dataType].clear(fid)
		zap.L().Info("archived log file gc ends", zap.Uint32("fid", fid))
	}

	return nil
}

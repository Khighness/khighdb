package khighdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Khighness/khighdb/storage"
	"github.com/Khighness/khighdb/util"
)

// @Author KHighness
// @Update 2023-01-01

func (db *KhighDB) initDiscard() error {
	discardPath := filepath.Join(db.options.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	for i := String; i < logFileTypeNum; i++ {
		name := storage.FileNamesMap[storage.FileType(i)] + discardFileName
		discard, err := newDiscard(discardPath, name, db.options.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = discard
	}
	db.discards = discards
	return nil
}

func (db *KhighDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	fileInfos, err := ioutil.ReadDir(db.options.DBPath)
	if err != nil {
		return err
	}

	fidMap := make(map[DataType][]uint32)
	for _, file := range fileInfos {
		if strings.HasPrefix(file.Name(), storage.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			dataType := DataType(storage.FileTypesMap[splitNames[1]])
			fidMap[dataType] = append(fidMap[dataType], uint32(fid))
		}
	}
	db.fidMap = fidMap

	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// Load log files in order.
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		options := db.options
		for i, fid := range fids {
			fileType, ioType := storage.FileType(dataType), storage.IOType(options.IoType)
			logFile, err := storage.OpenLogFile(options.DBPath, fid, options.LogFileSizeThreshold, fileType, ioType)
			if err != nil {
				return err
			}
			// Lastest one is active log file.
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = logFile
			} else {
				db.archivedLogFiles[dataType][fid] = logFile
			}
		}
	}

	return nil
}

func (db *KhighDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	options := db.options
	fileType, ioType := storage.FileType(dataType), storage.IOType(options.IoType)
	logFile, err := storage.OpenLogFile(options.DBPath, storage.InitialLogField, options.LogFileSizeThreshold, fileType, ioType)
	if err != nil {
		return err
	}

	db.discards[dataType].setTotal(logFile.Fid, uint32(options.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = logFile
	return nil
}

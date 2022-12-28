package khighdb

import (
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.com/khighness/khighdb/ioselector"
	"github.com/khighness/khighdb/storage"
)

// @Author KHighness
// @Update 2022-12-27

const (
	// discardRecordSize is the size of a discard record.
	//	size(fid) + size(total size) + size(discarded size) = 12
	discardRecordSize = 12
	// discardFileSize is the size of the discard file.
	//	8KB, contains (8192 / 12 = 682) records.
	discardFileSize int64 = 8 << 10
	// discardFileName is the name of the discard file.
	discardFileName = "discard"
)

// ErrDiscardNoSpace represents there is no enough space for discard file.
var ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")

// discard is used to record total size and discarded size in a log file.
// Mainly for log files compaction.
//	The structure of discard file:
//	+-------+--------------+--------------+ +-------+--------------+--------------+
//	|  fid  |  total size  | discard size | |  fid  |  total size  | discard size |
//	+-------+--------------+--------------+ +-------+--------------+--------------+
//	0-------4--------------8-------------12 12------16-------------20------------24
type discard struct {
	sync.Mutex
	once     *sync.Once
	valChan  chan *indexNode
	file     ioselector.IOSelector
	freeList []int64          // contains file offset that can be allocated
	location map[uint32]int64 // offset of each fid
}

// newDiscard creates a discard internally.
func newDiscard(path, name string, bufferSize int) (*discard, error) {
	fileName := filepath.Join(path, name)
	file, err := ioselector.NewMMapSelector(fileName, discardFileSize)
	if err != nil {
		return nil, err
	}

	var freeList []int64
	var offset int64
	location := make(map[uint32]int64)
	for {
		// Read fid and total size.
		buf := make([]byte, 8)
		if _, err = file.Read(buf, offset); err != nil {
			if err == io.EOF || err == storage.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		fid := binary.LittleEndian.Uint32(buf[:4])
		totalSize := binary.LittleEndian.Uint32(buf[4:8])
		if fid == 0 && totalSize == 0 {
			freeList = append(freeList, offset)
		} else {
			location[fid] = offset
		}
		offset += discardRecordSize
	}

	d := &discard{
		once:     new(sync.Once),
		valChan:  make(chan *indexNode, bufferSize),
		file:     file,
		freeList: freeList,
		location: location,
	}
	go d.listenUpdates()
	return d, nil
}

func (d *discard) sync() error {
	return d.file.Sync()
}

func (d *discard) close() error {
	return d.file.Close()
}

// getCCL returns the compaction candidate list.
// Iterate and find the file with most discarded data.
// There are 682 records at most, regardless of performance.
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	var offset int64
	var ccl []uint32
	d.Lock()
	defer d.Unlock()
	for {
		buf := make([]byte, discardRecordSize)
		_, err := d.file.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == storage.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		offset += discardRecordSize

		fid := binary.LittleEndian.Uint32(buf[:4])
		totalSize := binary.LittleEndian.Uint32(buf[4:8])
		discardSize := binary.LittleEndian.Uint32(buf[8:12])
		var curRatio float64
		if totalSize != 0 && discardSize != 0 {
			curRatio = float64(discardSize) / float64(totalSize)
		}
		if curRatio >= ratio && fid != activeFid {
			ccl = append(ccl, fid)
		}
	}

	// Sort by fid in ascending order, guarantee the older file will be compacted firstly.
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

func (d *discard) listenUpdates() {
	for {
		select {
		case idxNode, ok := <-d.valChan:
			if !ok {
				if err := d.file.Close(); err != nil {
					zap.L().Error("failed to close discard file", zap.Error(err))
				}
				return
			}
			d.incrDiscard(idxNode.fid, idxNode.entrySize)
		}
	}
}

func (d *discard) closeChan() {
	d.once.Do(func() {
		close(d.valChan)
	})
}

func (d *discard) setTotal(fid uint32, totalSize uint32) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.location[fid]; ok {
		zap.L().Info("skip setting total size due to duplicate setting", zap.Uint32("fid", fid))
		return
	}
	offset, err := d.alloc(fid)
	if err != nil {
		zap.L().Error("discard file allocate err", zap.Uint32("fid", fid), zap.Error(err))
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], totalSize)
	if _, err = d.file.Write(buf, offset); err != nil {
		zap.L().Error("set total size in discard file err", zap.Uint32("fid", fid), zap.Error(err))
		return
	}
	zap.L().Info("set total size in discard file",
		zap.Uint32("fid", fid), zap.Uint32("totalSize", totalSize))
}

func (d *discard) clear(fid uint32) {
	d.incr(fid, -1)
	d.Lock()
	defer d.Unlock()
	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		delete(d.location, fid)
	}
}

func (d *discard) incrDiscard(fid uint32, entrySize int) {
	if entrySize > 0 {
		d.incr(fid, entrySize)
	}
}

func (d *discard) incr(fid uint32, delta int) {
	d.Lock()
	defer d.Unlock()

	offset, err := d.alloc(fid)
	if err != nil {
		zap.L().Error("discard file allocate err", zap.Uint32("fid", fid), zap.Error(err))
		return
	}

	var buf []byte
	if delta > 0 {
		buf = make([]byte, 4)
		offset += 8
		if _, err = d.file.Read(buf, offset); err != nil {
			zap.L().Error("read discard size in discard file err", zap.Uint32("fid", fid), zap.Error(err))
			return
		}

		discardSize := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, discardSize+uint32(delta))
		zap.L().Info("incr discard size", zap.Uint32("fid", fid),
			zap.Uint32("discardSize", discardSize), zap.Int("delta", delta))
	} else {
		buf = make([]byte, discardRecordSize)
	}

	if _, err = d.file.Write(buf, offset); err != nil {
		zap.L().Error("incr discard size in discard file err", zap.Uint32("fid", fid), zap.Error(err))
		return
	}
}

func (d *discard) alloc(fid uint32) (int64, error) {
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	if len(d.freeList) == 0 {
		return 0, ErrDiscardNoSpace
	}

	offset := d.freeList[len(d.freeList)-1]
	d.freeList = d.freeList[:len(d.freeList)-1]
	d.location[fid] = offset
	return offset, nil
}

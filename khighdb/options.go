package khighdb

import "time"

// @Author KHighness
// @Update 2022-12-26

// DataIndexMode defines the data index mode.
type DataIndexMode int8

const (
	// KeyValueMemMode represents key and value are both in memory, read operation
	// will be very fast in this mode.
	// This mode is suitable for scenarios where the values are relatively small.
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode represents only keep keys in memory, there is a disk seek
	// while getting a value because values are in log file on disk.
	KeyOnlyMemMode
)

// IOType defines the I/O type.
type IOType int8

const (
	// FileIO represents using standard file I/O.
	FileIO IOType = iota

	// MMap represents using memory-mapped buffer.
	MMap
)

// Options defines the options for opening a KhighDB.
type Options struct {
	// DBPath is the path of db, which will be created automatically if not exist.
	DBPath string

	// IndexMode is the mode of index, support KeyValueMemMode and KeyOnlyMemMode now.
	// Note that this mode is noly for KV pairs, not for List, Hash, Set and ZSet.
	// Default value is KeyOnlyMemMode.
	IndexMode DataIndexMode

	// IoType is the type of I/O, support FileIO abd MMap now.
	// Default value is FileIO.
	IoType IOType

	// Sync is whether to synchronize writes from the OS buffer cache through to disk.
	// If this value is false, some recent writes may be lost when the machine crashes.
	// Note that if it is just the process crashes but the machine does not then no writes
	// will be lost.
	// Default value is false.
	Sync bool

	// LogFileGCInternal is the internal for a background goroutine to execute log file
	// garbage collection periodically. It will pick the log file that meets the condition
	// for GC, then rewrite the valid data one by one.
	// Default value is 8 hours.
	LogFileGCInternal time.Duration

	// LogFileGCRatio means if discarded data in log file exceeds this ratio, it can be
	// picked up for compaction. And if there are many files reached the ratio, we will
	// pick the highest one by one.
	// The recommended ratio is 0.5, half of the file can be compacted.
	// Default value is 0.5.
	LogFileGCRatio float64

	// LogFileSizeThreshold is the threshold size of each log file, active log file will
	// be closed if file reaches the threshild.
	// Note that this value must be set to the same as the first startup.
	// Default value is 512MB.
	LogFileSizeThreshold int64

	// DiscardBufferSize is the max size for the discard log entries.
	// A channel will be created to send the older entry size when a key is updated or deleted.
	// Entry size will be saved in the discard file, recoding the invalid size in a log file,
	// and be used for log file gc.
	// Default value is 8MB.
	DiscardBufferSize int
}

// DefaultOptions returns the default options for opening a KhighDB.
func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		IndexMode:            KeyOnlyMemMode,
		IoType:               FileIO,
		Sync:                 false,
		LogFileGCInternal:    8 * time.Hour,
		LogFileGCRatio:       0.5,
		LogFileSizeThreshold: 512 << 20,
		DiscardBufferSize:    8 << 20,
	}
}

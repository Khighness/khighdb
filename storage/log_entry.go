package storage

import (
	"encoding/binary"
	"hash/crc32"
)

// @Author KHighness
// @Update 2022-12-25

// MaxHeaderSize defines the max size of entry header.
//	The structure of entry header is as follows:
//	+-----------+-----------+-----------+-----------+-----------+
//	|   crc32   |    type   |  KeySize  | ValueSize | expiredAt |
//	+-----------+-----------+-----------+-----------+-----------+
//	|   uint32  |    byte   |   uint32  |   uint32  |   int64   |
//	+-----------+-----------+-----------+-----------+-----------+
//	So, MaxHeaderSize = 4 + 1 + 5 + 5 + 10 = 25
const MaxHeaderSize = 25

// EntryType defines the type of log entry.
type EntryType byte

const (
	// TypeDelete represents entry type is delete.
	TypeDelete EntryType = iota + 1
	// TypeListMeta represents entry is list meta.
	TypeListMeta
)

// LogEntry is the data which will be appended in log file.
type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64
	Type      EntryType
}

// entryMeta define the structure of log entry's meta info.
type entryMeta struct {
	crc32     uint32 // check sum
	typ       EntryType
	keySize   uint32
	valSize   uint32
	expiredAt int64 // time.Unix
}

// EncodeEntry will encode entry into a byte slice.
//	The encoded entry looks like:
//	+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
//	|   crc32   |    type   |  KeySize  | ValueSize | expiredAt |    key    |   value   |
//	+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
//	|<------------------------META INFO------------------------>|
//              |<---------------------------CRC CHECK SUM---------------------------->|
func EncodeEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}

	meta := make([]byte, MaxHeaderSize)
	meta[4] = byte(e.Type)

	var index = 5
	index += binary.PutVarint(meta[index:], int64(len(e.Key)))
	index += binary.PutVarint(meta[index:], int64(len(e.Value)))
	index += binary.PutVarint(meta[index:], e.ExpiredAt)

	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	copy(buf[:index], meta[:])
	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func decodeMeta(buf []byte) (*entryMeta, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	meta := &entryMeta{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])
	meta.keySize = uint32(keySize)
	index += n

	valSize, n := binary.Varint(buf[index:])
	meta.valSize = uint32(valSize)
	index += n

	expiredAT, n := binary.Varint(buf[index:])
	meta.expiredAt = expiredAT
	return meta, int64(index + n)
}

func getEntryCrc(e *LogEntry, m []byte) uint32 {
	if e == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(m[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}

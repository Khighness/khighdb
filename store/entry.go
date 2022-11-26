package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

// @Author KHighness
// @Update 2022-11-16

var (
	// ErrInvalidEntry represents invalid entry.
	ErrInvalidEntry = errors.New("store|entry: invalid entry")
	// ErrInvalidCrc represents invalid crc.
	ErrInvalidCrc = errors.New("store|entry: invalid crc")
)

// data structure type of value, support the following five types.
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

// entryHeaderSize is the sum of entry header bytes.
// The structure of entry header is as follows:
// +-----------+-----------+-----------+-----------+-----------+-----------+-----------+
// |   crc32   |  KeySize  | ValueSize | ExtraSize |   state   | Timestamp |    TxId   |
// +-----------+-----------+-----------+-----------+-----------+-----------+-----------+
// |   uint16  |   uint16  |   uint16  |   uint16  |   uint8   |   uint32  |   uint32  |
// +-----------+-----------+-----------+-----------+-----------+-----------+-----------+
// So, entryHeaderSize = 4 + 4 + 4 + 4 + 2 + 8 + 8 = 34.
const entryHeaderSize = 34

// Meta defines the structure of the entry's metadata.
type Meta struct {
	Key       []byte
	Value     []byte
	Extra     []byte
	KeySize   uint32
	ValueSize uint32
	ExtraSize uint32
}

// Entry defines the structure of the entry.
type Entry struct {
	Meta      *Meta
	state     uint16 // state contains two fields, high b bits is the data type, low b bits is operation mark.
	crc32     uint32 // check sum.
	TimeStamp uint64 // TimeStamp is the time when entry was written.
	TxId      uint64 // TxId represents transaction id of the entry.
}

// newInternal creates a new entry internally.
func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {
	return &Entry{
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
		state:     state,
		TimeStamp: timestamp,
	}
}

// NewEntry creates a new entry.
func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	var state uint16 = 0
	// set data type and operation mark
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, value, extra, state, uint64(time.Now().UnixNano()))
}

// NewEntryWithoutExtra creates a new entry without extra info.
func NewEntryWithoutExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

// NewEntryWithExpire creates a new entry with expired info.
func NewEntryWithExpire(key, value []byte, deadline int64, t, mark uint16) *Entry {
	var state uint16 = 0
	// set data type and operation mark
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, value, nil, state, uint64(deadline))
}

// NewEntryWithTxn creates a new entry with transaction info.
// TODO fix
func NewEntryWithTxn(key, value, extra []byte, t, mark uint16, txId uint64) *Entry {
	e := NewEntry(key, value, extra, t, mark)
	e.TxId = txId
	return e
}

// Size returns the entry's total size.
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ValueSize
}

// GetType returns the data type of the entry.
func (e *Entry) GetType() uint16 {
	return e.state >> 8
}

// GetMark returns the operation mark of the entry.
func (e *Entry) GetMark() uint16 {
	return e.state & (2<<7 - 1)
}

// Encode encodes the entry into a byte array.
func Encode(e *Entry) ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.state)
	binary.BigEndian.PutUint64(buf[18:26], e.TimeStamp)
	binary.BigEndian.PutUint64(buf[26:34], e.TxId)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:entryHeaderSize+ks+vs], e.Meta.Value)

	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// Decode decodes the byte array into an entry.
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	state := binary.BigEndian.Uint16(buf[16:18])
	timestamp := binary.BigEndian.Uint64(buf[18:26])
	txId := binary.BigEndian.Uint64(buf[26:34])
	crc := binary.BigEndian.Uint32(buf[0:4])

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		state:     state,
		crc32:     crc,
		TimeStamp: timestamp,
		TxId:      txId,
	}, nil
}

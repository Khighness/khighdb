package storage

import (
	"reflect"
	"testing"
)

// @Author KHighness
// @Update 2022-12-25

func TestEncodeEntry(t *testing.T) {
	type args struct {
		e *LogEntry
	}
	tests := []struct {
		name string
		args args
		want []byte
		len  int
	}{
		{
			"nil", args{e: nil}, nil, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}}, []byte{28, 223, 68, 33, 0, 0, 0, 0}, 8,
		},
		{
			"no-key-value", args{e: &LogEntry{ExpiredAt: 8327587356287}}, []byte{148, 28, 238, 48, 0, 0, 0, 254, 249, 150, 174, 221, 228, 3}, 14,
		},
		{
			"with-key-value", args{e: &LogEntry{Key: []byte("key"), Value: []byte("value"), ExpiredAt: 834758743589437598}}, []byte{47, 51, 61, 166, 0, 6, 10, 188, 130, 230, 140, 242, 169, 212, 149, 23, 107, 101, 121, 118, 97, 108, 117, 101}, 24,
		},
		{
			"type-delete", args{e: &LogEntry{Key: []byte("key"), Value: []byte("value"), ExpiredAt: 834758743589437598, Type: TypeDelete}}, []byte{105, 8, 90, 195, 1, 6, 10, 188, 130, 230, 140, 242, 169, 212, 149, 23, 107, 101, 121, 118, 97, 108, 117, 101}, 24,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, n := EncodeEntry(tt.args.e)
			if !reflect.DeepEqual(buf, tt.want) {
				t.Errorf("EncodeEntry() buf: actual = %v, expected = %v", buf, tt.want)
			}
			if n != tt.len {
				t.Errorf("EncodeEntry() len: actual = %v, expected = %v", n, tt.len)
			}
		})
	}
}

func Test_decodeMeta(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
		want *entryMeta
		len  int
	}{
		{
			"nil", args{buf: nil}, nil, 0,
		},
		{
			"no-enough-bytes", args{buf: []byte{1, 4, 3, 22}}, nil, 0,
		},
		{
			"no-fields", args{buf: []byte{28, 223, 68, 33, 0, 0, 0, 0}}, &entryMeta{crc32: 558161692}, 8,
		},
		{
			"normal", args{buf: []byte{101, 208, 223, 156, 0, 4, 14, 198, 147, 242, 166, 3}}, &entryMeta{crc32: 2631913573, typ: 0, keySize: 2, valSize: 7, expiredAt: 443434211}, 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, n := decodeMeta(tt.args.buf)
			if !reflect.DeepEqual(meta, tt.want) {
				t.Errorf("decodeHeader() buf: got = %v, want %v", meta, tt.want)
			}
			if int(n) != tt.len {
				t.Errorf("decodeHeader() len: got1 = %v, want %v", n, tt.len)
			}
		})
	}
}

func Test_getEntryCrc(t *testing.T) {
	type args struct {
		e *LogEntry
		h []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			"nil", args{e: nil, h: nil}, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}, h: []byte{0, 0, 0, 0}}, 558161692,
		},
		{
			"normal", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("khighdb")}, h: []byte{0, 4, 14, 198, 147, 242, 166, 3}}, 913202917,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getEntryCrc(tt.args.e, tt.args.h); got != tt.want {
				t.Errorf("getEntryCrc() = %v, want %v", got, tt.want)
			}
		})
	}
}

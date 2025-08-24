package rdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"strconv"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/utils"
)

type rdbLoader struct {
	reader *bytes.Reader
	kv     *kv.KV
}

func newLoader(data []byte, kv *kv.KV) *rdbLoader {
	return &rdbLoader{
		reader: bytes.NewReader(data),
		kv:     kv,
	}
}

func Load(path string, kv *kv.KV) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("RDB file does not exist")
	}
	data, err := readFileAndVerifyChecksum(path)
	if err != nil {
		return err
	}
	loader := newLoader(data, kv)

	if err := loader.loadHeader(); err != nil {
		return err
	}
	if err := loader.loadData(); err != nil {
		return err
	}

	return nil
}

func readFileAndVerifyChecksum(path string) ([]byte, error) {
	file, err := os.Open(path)

	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	if fileInfo.Size() < 18 {
		return nil, fmt.Errorf("rdb file is too small")
	}

	buf := make([]byte, fileInfo.Size())
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, err
	}
	storedChecksum := binary.BigEndian.Uint64(buf[len(buf)-8:])
	data := buf[:len(buf)-8]
	calculatedChecksum := crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("rdb checksum verification failed")
	}
	return data, nil
}

func (l *rdbLoader) loadHeader() error {
	magic := make([]byte, len(MagicString))
	if _, err := io.ReadFull(l.reader, magic); err != nil {
		return err
	}
	if string(magic) != MagicString {
		return fmt.Errorf("invalid rdb magic string")
	}
	var version uint64
	if err := binary.Read(l.reader, binary.BigEndian, &version); err != nil {
		return err
	}
	if version != Version {
		return fmt.Errorf("unsupported rdb version: %d", version)
	}
	return nil
}

func (l *rdbLoader) loadData() error {
	for {
		opcode := make([]byte, 1)
		_, err := io.ReadFull(l.reader, opcode)
		if err != nil {
			return fmt.Errorf("unexpected eof before rdb eof opcode")
		}
		if opcode[0] == OpCodeEOF {
			return nil
		}
		if err := l.loadObject(opcode[0]); err != nil {
			return err
		}
	}
}

func (l *rdbLoader) loadObject(opcode byte) error {
	switch opcode {
	case OpCodeString:
		return l.loadStringObject()
	case OpCodeList:
		return l.loadListObject()
	case OpCodeHash:
		return l.loadHashObject()
	case OpCodeZSet:
		return l.loadZSetObject()
	case OpCodeStream:
		return l.loadStreamObject()
	default:
		return fmt.Errorf("unknown opcode: %x", opcode)
	}
}

func (l *rdbLoader) loadStringObject() error {
	key, err := ReadString(l.reader)
	if err != nil {
		return err
	}
	val, err := ReadString(l.reader)
	if err != nil {
		return err
	}
	l.kv.SETs[key] = resp.Value{Typ: "bulk", Bulk: val}
	return nil
}

func (l *rdbLoader) loadListObject() error {
	key, err := ReadString(l.reader)
	if err != nil {
		return err
	}

	var count uint64
	if err := binary.Read(l.reader, binary.BigEndian, &count); err != nil {
		return err
	}

	list := make([]resp.Value, count)
	for i := uint64(0); i < count; i++ {
		item, err := ReadString(l.reader)
		if err != nil {
			return err
		}
		list[i] = resp.Value{Typ: "bulk", Bulk: item}
	}
	l.kv.Lists[key] = list
	return nil
}

func (l *rdbLoader) loadHashObject() error {
	key, err := ReadString(l.reader)
	if err != nil {
		return err
	}

	var fieldCount uint64
	if err := binary.Read(l.reader, binary.BigEndian, &fieldCount); err != nil {
		return err
	}

	fields := make(map[string]resp.Value, fieldCount)
	for i := uint64(0); i < fieldCount; i++ {
		field, err := ReadString(l.reader)
		if err != nil {
			return err
		}
		value, err := ReadString(l.reader)
		if err != nil {
			return err
		}
		fields[field] = resp.Value{Typ: "bulk", Bulk: value}
	}
	l.kv.Hashes[key] = fields
	return nil
}

func (l *rdbLoader) loadZSetObject() error {
	key, err := ReadString(l.reader)
	if err != nil {
		return err
	}

	var memberCount uint64
	if err := binary.Read(l.reader, binary.BigEndian, &memberCount); err != nil {
		return err
	}

	members := make(map[string]float64, memberCount)
	for i := uint64(0); i < memberCount; i++ {
		member, err := ReadString(l.reader)
		if err != nil {
			return err
		}
		scoreStr, err := ReadString(l.reader)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return err
		}
		members[member] = score
	}
	l.kv.Sorteds[key] = members
	return nil
}

func (l *rdbLoader) loadStreamObject() error {
	key, err := ReadString(l.reader)
	if err != nil {
		return err
	}

	var entryCount uint64
	if err := binary.Read(l.reader, binary.BigEndian, &entryCount); err != nil {
		return err
	}

	stream := &kv.Stream{}
	entries := make([]kv.StreamEntry, entryCount)
	for i := range entries {
		id, err := ReadString(l.reader)
		if err != nil {
			return err
		}

		var fieldCount uint64
		if err := binary.Read(l.reader, binary.BigEndian, &fieldCount); err != nil {
			return err
		}

		fields := make(map[string]resp.Value, fieldCount)
		for j := uint64(0); j < fieldCount; j++ {
			field, err := ReadString(l.reader)
			if err != nil {
				return err
			}
			value, err := ReadString(l.reader)
			if err != nil {
				return err
			}
			fields[field] = resp.Value{Typ: "bulk", Bulk: value}
		}

		parsedID, err := utils.ParseStreamID(id)
		if err != nil {
			return err
		}
		entries[i] = kv.StreamEntry{ID: parsedID, Fields: fields}
	}
	stream.Entries = entries
	l.kv.Streams[key] = stream
	return nil
}

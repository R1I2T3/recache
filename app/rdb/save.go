package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"

	"github.com/r1i2t3/go-redis/app/kv"
)

func Save(path string, kv *kv.KV) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := bufio.NewWriter(file)
	hasher := crc64.New(crc64.MakeTable(crc64.ISO))
	writer := io.MultiWriter(buf, hasher)

	if err := writeHeader(writer); err != nil {
		return fmt.Errorf("failed to write rdb header: %w", err)
	}
	if err := saveStrings(writer, kv); err != nil {
		return fmt.Errorf("failed to save strings: %w", err)
	}
	if err := saveLists(writer, kv); err != nil {
		return fmt.Errorf("failed to save lists: %w", err)
	}
	if err := saveHashes(writer, kv); err != nil {
		return fmt.Errorf("failed to save hashes: %w", err)
	}
	if err := saveStreams(writer, kv); err != nil {
		return fmt.Errorf("failed to save streams: %w", err)
	}
	if err := saveSortedSrings(writer, kv); err != nil {
		return fmt.Errorf("failed to save sorted Srings: %w", err)
	}
	if err := writeFooter(writer, buf, hasher); err != nil {
		return fmt.Errorf("failed to write rdb footer: %w", err)
	}
	return buf.Flush()
}

func SaveToBuffer(kv *kv.KV) ([]byte, error) {
	var buf bytes.Buffer
	hasher := crc64.New(crc64.MakeTable(crc64.ISO))
	writer := io.MultiWriter(&buf, hasher)

	if err := writeHeader(writer); err != nil {
		return nil, fmt.Errorf("failed to write rdb header: %w", err)
	}
	if err := saveStrings(writer, kv); err != nil {
		return nil, fmt.Errorf("failed to save strings: %w", err)
	}
	if err := saveLists(writer, kv); err != nil {
		return nil, fmt.Errorf("failed to save lists: %w", err)
	}
	if err := saveHashes(writer, kv); err != nil {
		return nil, fmt.Errorf("failed to save hashes: %w", err)
	}
	if err := saveStreams(writer, kv); err != nil {
		return nil, fmt.Errorf("failed to save streams: %w", err)
	}
	if err := saveSortedSrings(writer, kv); err != nil {
		return nil, fmt.Errorf("failed to save sorted Srings: %w", err)
	}
	if _, err := writer.Write([]byte{OpCodeEOF}); err != nil {
		return nil, fmt.Errorf("failed to write rdb eof marker: %w", err)
	}

	checksum := hasher.Sum64()
	if err := binary.Write(&buf, binary.BigEndian, checksum); err != nil {
		return nil, fmt.Errorf("failed to write rdb checksum: %w", err)
	}

	return buf.Bytes(), nil
}

func writeHeader(writer io.Writer) error {
	if _, err := writer.Write([]byte(MagicString)); err != nil {
		return err
	}
	return binary.Write(writer, binary.BigEndian, uint32(Version))
}

func writeFooter(writer io.Writer, buf *bufio.Writer, hasher hash.Hash64) error {
	if _, err := writer.Write([]byte{OpCodeEOF}); err != nil {
		return err
	}
	checksum := hasher.Sum64()
	return binary.Write(buf, binary.BigEndian, checksum)
}

func saveStrings(writer io.Writer, kv *kv.KV) error {
	kv.StringsMu.RLock()
	defer kv.StringsMu.RUnlock()

	for key, value := range kv.Strings {
		if _, err := writer.Write([]byte{OpCodeString}); err != nil {
			return err
		}
		if err := WriteString(writer, key); err != nil {
			return err
		}
		if err := WriteString(writer, value.Str); err != nil {
			return err
		}
	}
	return nil
}

func saveLists(writer io.Writer, kv *kv.KV) error {
	kv.ListsMu.RLock()
	defer kv.ListsMu.RUnlock()

	for key, list := range kv.Lists {
		if _, err := writer.Write([]byte{OpCodeList}); err != nil {
			return err
		}
		if err := WriteString(writer, key); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, uint64(len(list))); err != nil {
			return err
		}
		for _, item := range list {
			fmt.Println(item.Bulk)
			if err := WriteString(writer, item.Bulk); err != nil {
				return err
			}
		}
	}
	return nil
}

func saveHashes(writer io.Writer, kv *kv.KV) error {
	kv.HashesMu.RLock()
	defer kv.HashesMu.RUnlock()

	for key, hash := range kv.Hashes {
		if _, err := writer.Write([]byte{OpCodeHash}); err != nil {
			return err
		}
		if err := WriteString(writer, key); err != nil {
			return err
		}

		if err := binary.Write(writer, binary.BigEndian, uint64(len(hash))); err != nil {
			return err
		}

		for field, value := range hash {
			if err := WriteString(writer, field); err != nil {
				return err
			}
			if err := WriteString(writer, value.Bulk); err != nil {
				return err
			}
		}
	}
	return nil
}

func saveStreams(writer io.Writer, kv *kv.KV) error {
	kv.StreamsMu.RLock()
	defer kv.StreamsMu.RUnlock()

	for key, stream := range kv.Streams {
		if _, err := writer.Write([]byte{OpCodeStream}); err != nil {
			return err
		}
		if err := WriteString(writer, key); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, uint64(len(stream.Entries))); err != nil {
			return err
		}
		for _, entry := range stream.Entries {
			if err := WriteString(writer, entry.ID.ToString()); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, uint64(len(entry.Fields))); err != nil {
				return err
			}
			for field, value := range entry.Fields {
				if err := WriteString(writer, field); err != nil {
					return err
				}
				if err := WriteString(writer, value.Bulk); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func saveSortedSrings(writer io.Writer, kv *kv.KV) error {
	kv.SortedsMu.RLock()
	defer kv.SortedsMu.RUnlock()

	for key, sortedSet := range kv.Sorteds {
		if _, err := writer.Write([]byte{OpCodeZSet}); err != nil {
			return err
		}
		if err := WriteString(writer, key); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, uint64(len(sortedSet))); err != nil {
			return err
		}
		for member, score := range sortedSet {
			if err := WriteString(writer, member); err != nil {
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, score); err != nil {
				return err
			}
		}
	}
	return nil
}

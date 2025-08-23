package rdb

import (
	"encoding/binary"
	"io"
)

const (
	MagicString = "REDIS"
	Version     = 1
)

const (
	OpCodeString byte = 0
	OpCodeList   byte = 1
	OpCodeHash   byte = 2
	OpCodeSet    byte = 3
	OpCodeZSet   byte = 4
	OpCodeStream byte = 5

	OpCodeDBSelector byte = 0xFB
	OpCodeExpireTime byte = 0xFD
	OpCodeEOF        byte = 0xFF
)

func WriteString(w io.Writer, s string) error {
	if err := binary.Write(w, binary.BigEndian, uint64(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func ReadString(r io.Reader) (string, error) {
	var length uint64
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING = '+'
	ERROR  = '-'
	NUMBER = ':'
	BULK   = '$'
	ARRAY  = '*'
)

var (
	ErrInvalidPrefix = errors.New("resp: invalid type prefix")
	ErrShortBulk     = errors.New("resp: short bulk payload")
	ErrLineTooLong   = errors.New("resp: line too long")
)

type Value struct {
	Typ     string
	Str     string
	Err     string
	Num     int
	Bulk    string
	Array   []Value
	Expires int64
}

type RespError struct {
	Msg string
}

type Parser struct {
	r       *bufio.Reader
	maxLine int
}

func (e RespError) Error() string {
	return e.Msg
}

func NewParser(rd io.Reader) *Parser {
	return &Parser{
		r:       bufio.NewReader(rd),
		maxLine: 512 * 1024,
	}
}

func (p *Parser) Parse() (Value, error) {
	prefix, err := p.r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch prefix {
	case STRING:
		return p.parseString()
	case ERROR:
		return p.parseError()
	case NUMBER:
		return p.parseInt()
	case BULK:
		return p.parseBulkString()
	case ARRAY:
		return p.parseArray()
	default:
		return Value{}, ErrInvalidPrefix
	}
}

func (p *Parser) parseString() (Value, error) {
	str, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Typ: "string", Str: str}, nil
}

func (p *Parser) parseError() (Value, error) {
	s, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Typ: "error", Err: s}, nil
}

func (p *Parser) parseInt() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	num, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, err
	}
	return Value{Typ: "number", Num: int(num)}, nil
}

func (p *Parser) parseBulkString() (Value, error) {
	lenStr, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(lenStr, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("resp: bad bulk length: %w", err)
	}
	if n == -1 {
		return Value{}, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(p.r, buf); err != nil {
		return Value{}, ErrShortBulk
	}
	if err := p.expectCRLF(); err != nil {
		return Value{}, err
	}
	return Value{Typ: "bulk", Bulk: string(buf)}, nil
}

func (p *Parser) parseArray() (Value, error) {
	lenStr, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(lenStr, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("resp: bad array length: %w", err)
	}
	if n == -1 {
		return Value{}, nil
	}
	arr := make([]Value, n)
	for i := int64(0); i < n; i++ {
		elem, err := p.Parse()
		if err != nil {
			return Value{}, err
		}
		arr[i] = elem
	}
	return Value{Typ: "array", Array: arr}, nil
}

func (p *Parser) readLine() (string, error) {
	var b []byte
	for {
		part, isPrefix, err := p.r.ReadLine()
		if err != nil {
			return "", nil
		}
		b = append(b, part...)
		if len(b) > p.maxLine {
			return "", ErrLineTooLong
		}
		if !isPrefix {
			break
		}
	}
	return string(b), nil
}

func (d *Parser) expectCRLF() error {
	c1, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	c2, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	if c1 != '\r' || c2 != '\n' {
		return fmt.Errorf("resp: expected CRLF")
	}
	return nil
}

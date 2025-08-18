package main

import "io"

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w Writer) Write(v Value) error {
	_, err := w.writer.Write(v.Serializer())
	return err
}

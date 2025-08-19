package writer

import (
	"io"

	"github.com/r1i2t3/go-redis/app/resp"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w Writer) Write(v resp.Value) error {
	_, err := w.writer.Write(v.Serializer())
	return err
}

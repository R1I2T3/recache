package writer

import (
	"fmt"
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

func (w *Writer) WriteRDB(rdbBuffer []byte) error {
	prefix := fmt.Sprintf("$%d\r\n", len(rdbBuffer))
	if _, err := w.writer.Write([]byte(prefix)); err != nil {
		return err
	}
	fmt.Println(rdbBuffer)
	if _, err := w.writer.Write(rdbBuffer); err != nil {
		return err
	}
	return nil
}

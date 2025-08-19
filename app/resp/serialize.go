package resp

import "strconv"

func (v Value) Serializer() []byte {
	switch v.Typ {
	case "array":
		return v.serializeArray()
	case "string":
		return v.serializeString()
	case "error":
		return v.serializeError()
	case "bulk":
		return v.serializeBulk()
	case "null":
		return v.searializeNull()
	default:
		return []byte{}
	}

}

func (v Value) serializeArray() []byte {
	len := len(v.Array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')
	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].Serializer()...)
	}
	return bytes
}

func (v Value) serializeString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) serializeError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) serializeBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) searializeNull() []byte {
	return []byte("$-1\r\n")
}

package utils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func ParseStreamID(id string) (kv.StreamId, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		if len(parts) == 1 {
			timestamp, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				return kv.StreamId{}, err
			}
			return kv.StreamId{Timestamp: timestamp, Sequence: 0}, nil
		}
		return kv.StreamId{}, fmt.Errorf("invalid stream ID format")
	}

	timestamp, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return kv.StreamId{}, err
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return kv.StreamId{}, err
	}
	return kv.StreamId{Timestamp: timestamp, Sequence: sequence}, nil
}

func IsValidRequest(val resp.Value) bool {
	if val.Typ != "array" {
		fmt.Println("Invalid request, expected array")
		return false
	}
	if len(val.Array) == 0 {
		fmt.Println("Invalid request, expected array length > 0")
		return false
	}
	return true
}

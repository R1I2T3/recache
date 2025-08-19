package handlers

import (
	"fmt"
	"strings"
	"time"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func xadd(args []resp.Value, kV *kv.KV) resp.Value {
	if len(args) < 3 {
		return resp.Value{Typ: "Error", Bulk: "ERR wrong number of arguments for 'xadd' command"}
	}
	key := args[0].Bulk
	fieldsArray := args[1:]
	fields := make(map[string]resp.Value)
	for i := 0; i < len(fieldsArray); i += 2 {
		if i+1 >= len(fieldsArray) {
			return resp.Value{Typ: "Error", Bulk: "ERR wrong number of arguments for 'xadd' command"}
		}
		fields[fieldsArray[i].Bulk] = fieldsArray[i+1]
	}
	kV.StreamsMu.Lock()
	defer kV.StreamsMu.Unlock()
	stream, exists := kV.Streams[key]

	if !exists {
		stream = &kv.Stream{
			Entries: []kv.StreamEntry{},
			Groups:  make(map[string]*kv.ConsumerGroup),
		}
		kV.Streams[key] = stream
	}
	id := fmt.Sprintf("%d-%d", time.Now().UnixMilli(), len(stream.Entries)+1)
	entry := kv.StreamEntry{
		ID:     id,
		Fields: fields,
	}
	stream.Entries = append(stream.Entries, entry)
	return resp.Value{Typ: "bulk", Bulk: id}
}

func streamEntryToResp(entry kv.StreamEntry) resp.Value {
	fields := make([]resp.Value, 0, len(entry.Fields)*2)
	for k, v := range entry.Fields {
		fields = append(fields, resp.Value{Typ: "bulk", Bulk: k}, v)
	}
	return resp.Value{Typ: "array", Array: fields}
}

func xrange(args []resp.Value, kV *kv.KV) resp.Value {
	if len(args) < 3 {
		return resp.Value{Typ: "Error", Bulk: "ERR wrong number of arguments for 'xrange' command"}
	}
	key := args[0].Bulk
	start := args[1].Bulk
	end := args[2].Bulk
	kV.StreamsMu.RLock()
	defer kV.StreamsMu.RUnlock()
	stream, exists := kV.Streams[key]
	if !exists {
		return resp.Value{Typ: "Error", Bulk: "ERR no such key"}
	}
	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = "999999999999999999-999999"
	}
	var result []resp.Value
	for _, entry := range stream.Entries {
		stream := make([]resp.Value, 0, 2)
		stream = append(stream, resp.Value{Typ: "bulk", Bulk: entry.ID})
		if entry.ID >= start && entry.ID <= end {
			stream = append(stream, streamEntryToResp(entry))
		}
		result = append(result, resp.Value{Typ: "array", Array: stream})
	}
	fmt.Println("xrange result:", result)
	return resp.Value{Typ: "array", Array: result}
}

func xread(args []resp.Value, kV *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'xread' command"}
	}
	streams_key_word := args[0].Bulk
	key := args[1].Bulk
	field := args[2].Bulk
	kV.StreamsMu.RLock()
	defer kV.StreamsMu.RUnlock()
	stream, exists := kV.Streams[key]
	if !exists {
		return resp.Value{Typ: "error", Bulk: "ERR no such key"}
	}
	var result []resp.Value
	result = append(result, resp.Value{Typ: "bulk", Bulk: key})
	if strings.ToUpper(streams_key_word) != "STREAMS" {
		return resp.Value{Typ: "error", Bulk: "ERR syntax error"}
	}
	for _, entry := range stream.Entries {
		if entry.ID >= field {
			stream := make([]resp.Value, 0, 2)
			stream = append(stream, resp.Value{Typ: "bulk", Bulk: entry.ID})
			stream = append(stream, streamEntryToResp(entry))
			result = append(result, resp.Value{Typ: "array", Array: stream})
		}
	}
	return resp.Value{Typ: "array", Array: result}
}

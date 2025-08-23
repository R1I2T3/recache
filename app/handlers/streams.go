package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func parseStreamID(id string) (kv.StreamId, error) {
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

func xadd(args []resp.Value, kV *kv.KV) resp.Value {
	fmt.Println("xadd called with args:", len(args))
	if len(args) < 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'xadd' command"}
	}
	key := args[0].Bulk
	fieldsArray := args[1:]

	fields := make(map[string]resp.Value)
	for i := 0; i < len(fieldsArray); i += 2 {
		if i+1 >= len(fieldsArray) {
			return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'xadd' command"}
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

	id := kv.StreamId{Timestamp: uint64(time.Now().UnixMilli()), Sequence: uint64(len(stream.Entries) + 1)}
	entry := kv.StreamEntry{
		ID:     id,
		Fields: fields,
	}
	stream.Entries = append(stream.Entries, entry)
	incrementVersion(key, kV)
	kV.WakeUpClients(key, true)
	return resp.Value{Typ: "bulk", Bulk: id.ToString()}
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
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'xrange' command"}
	}
	key := args[0].Bulk
	start := args[1].Bulk
	end := args[2].Bulk
	kV.StreamsMu.RLock()
	defer kV.StreamsMu.RUnlock()
	stream, exists := kV.Streams[key]
	if !exists {
		return resp.Value{Typ: "error", Bulk: "ERR no such key"}
	}
	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = "999999999999999999-999999"
	}
	startID, err := parseStreamID(start)
	if err != nil {
		return resp.Value{Typ: "Error", Bulk: "ERR invalid start ID"}
	}
	endID, err := parseStreamID(end)
	if err != nil {
		return resp.Value{Typ: "Error", Bulk: "ERR invalid end ID"}
	}
	var result []resp.Value
	for _, entry := range stream.Entries {
		stream := make([]resp.Value, 0, 2)
		stream = append(stream, resp.Value{Typ: "bulk", Bulk: entry.ID.ToString()})
		if entry.ID.IsGreaterThan(startID) && entry.ID.IsSmallerThan(endID) {
			stream = append(stream, streamEntryToResp(entry))
		}
		result = append(result, resp.Value{Typ: "array", Array: stream})
	}
	fmt.Println("xrange result:", result)
	return resp.Value{Typ: "array", Array: result}
}

func xread(args []resp.Value, keyVal *kv.KV) resp.Value {
	if len(args) < 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'xread' command"}
	}
	var blockTimeout time.Duration = -1
	i := 0
	if strings.EqualFold(args[i].Bulk, "BLOCK") {
		i++
		if len(args) < i+2 {
			return resp.Value{Typ: "error", Str: "ERR syntax error"}
		}
		ms, err := strconv.Atoi(args[i].Bulk)
		if err != nil || ms < 0 {
			return resp.Value{Typ: "error", Str: "ERR invalid block timeout"}
		}
		blockTimeout = time.Duration(ms) * time.Millisecond
		i++
	}
	if strings.ToUpper(args[i].Bulk) != "STREAMS" {
		return resp.Value{Typ: "error", Str: "ERR syntax error"}
	}
	i++
	remainingArgs := len(args) - i
	if remainingArgs%2 != 0 || remainingArgs == 0 {
		return resp.Value{Typ: "error", Str: "ERR Unbalanced XREAD list of streams: keys and IDs must be specified in pairs."}
	}
	numStreams := remainingArgs / 2
	streamKeys := make([]string, numStreams)
	lastIDs := make(map[string]string, numStreams)
	for j := 0; j < numStreams; j++ {
		key := args[i+j].Bulk
		id := args[i+j+numStreams].Bulk
		streamKeys[j] = key
		lastIDs[key] = id
	}

RetryRead:
	keyVal.StreamsMu.RLock()
	finalResult := make([]resp.Value, 0)

	for _, key := range streamKeys {
		stream, exists := keyVal.Streams[key]
		if !exists {
			continue
		}

		lastIDStr := lastIDs[key]
		startID, err := parseStreamID(lastIDStr)
		if err != nil {
			keyVal.StreamsMu.RUnlock()
			return resp.Value{Typ: "error", Str: "ERR Invalid stream ID specified"}
		}

		streamEntries := make([]resp.Value, 0)
		for _, entry := range stream.Entries {
			if entry.ID.IsGreaterThan(startID) {
				entryResp := resp.Value{Typ: "array", Array: []resp.Value{
					{Typ: "bulk", Bulk: entry.ID.ToString()},
					streamEntryToResp(entry),
				}}
				streamEntries = append(streamEntries, entryResp)
			}
		}

		if len(streamEntries) > 0 {
			finalResult = append(finalResult, resp.Value{Typ: "array", Array: []resp.Value{
				{Typ: "bulk", Bulk: key},
				{Typ: "array", Array: streamEntries},
			}})
		}
	}
	keyVal.StreamsMu.RUnlock()

	if len(finalResult) > 0 {
		return resp.Value{Typ: "array", Array: finalResult}
	}
	if blockTimeout < 0 {
		return resp.Value{Typ: "null"}
	}
	if blockTimeout == 0 {
		blockTimeout = 365 * 24 * time.Hour
	}

	bc := &kv.BlockedClient{
		Ch:       make(chan bool, 1),
		Keys:     streamKeys,
		Deadline: time.Now().Add(blockTimeout),
		Context:  lastIDs,
	}
	keyVal.RegisterBlockedClient(bc)
	defer keyVal.UnregisterBlockedClient(bc)

	wokenUp := <-bc.Ch
	if wokenUp {
		goto RetryRead
	}
	return resp.Value{Typ: "null"}
}

package handlers

import (
	"fmt"
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

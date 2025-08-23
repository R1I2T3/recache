package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func hset(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}
	key := args[0].Bulk
	field := args[1].Bulk
	value := args[2].Bulk
	kv.HashesMu.Lock()
	defer kv.HashesMu.Unlock()
	if _, exists := kv.Hashes[key]; !exists {
		kv.Hashes[key] = make(map[string]resp.Value)
	}
	kv.Hashes[key][field] = resp.Value{Typ: "bulk", Bulk: value}
	incrementVersion(key, kv)
	return resp.Value{Typ: "integer", Num: 1}
}

func hget(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}
	key := args[0].Bulk
	field := args[1].Bulk
	kv.HashesMu.RLock()
	defer kv.HashesMu.RUnlock()
	if hash, exists := kv.Hashes[key]; exists {
		if value, exists := hash[field]; exists {
			return value
		}
	}
	return resp.Value{Typ: "null"}
}

func hdel(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hdel' command"}
	}
	key := args[0].Bulk
	field := args[1].Bulk
	kv.HashesMu.Lock()
	defer kv.HashesMu.Unlock()
	if hash, exists := kv.Hashes[key]; exists {
		if _, exists := hash[field]; exists {
			delete(hash, field)
			incrementVersion(key, kv)
			return resp.Value{Typ: "integer", Num: 1}
		}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

func hexists(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hexists' command"}
	}
	key := args[0].Bulk
	field := args[1].Bulk
	kv.HashesMu.RLock()
	defer kv.HashesMu.RUnlock()
	if hash, exists := kv.Hashes[key]; exists {
		if _, exists := hash[field]; exists {
			return resp.Value{Typ: "integer", Num: 1}
		}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

func hlen(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hlen' command"}
	}
	key := args[0].Bulk
	kv.HashesMu.RLock()
	defer kv.HashesMu.RUnlock()
	if hash, exists := kv.Hashes[key]; exists {
		return resp.Value{Typ: "integer", Num: int(len(hash))}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

func hkeys(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hkeys' command"}
	}
	key := args[0].Bulk
	kv.HashesMu.RLock()
	defer kv.HashesMu.RUnlock()
	if hash, exists := kv.Hashes[key]; exists {
		keys := make([]resp.Value, 0, len(hash))
		for field := range hash {
			keys = append(keys, resp.Value{Typ: "bulk", Bulk: field})
		}
		return resp.Value{Typ: "array", Array: keys}
	}
	return resp.Value{Typ: "array", Array: []resp.Value{}}
}

func hvals(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'hvals' command"}
	}
	key := args[0].Bulk
	kv.HashesMu.RLock()
	defer kv.HashesMu.RUnlock()
	if hash, exists := kv.Hashes[key]; exists {
		vals := make([]resp.Value, 0, len(hash))
		for _, v := range hash {
			vals = append(vals, v)
		}
		return resp.Value{Typ: "array", Array: vals}
	}
	return resp.Value{Typ: "array", Array: []resp.Value{}}
}

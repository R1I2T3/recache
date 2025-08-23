package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func ping(val []resp.Value, kv *kv.KV) resp.Value {
	if len(val) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}

	return resp.Value{Typ: "string", Str: val[0].Bulk}
}

func echo(val []resp.Value, kv *kv.KV) resp.Value {
	if len(val) == 2 && val[1].Typ == "bulk" {
		return resp.Value{Typ: "string", Str: val[1].Bulk}
	}
	return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'echo' command"}
}

func typeRedis(val []resp.Value, kv *kv.KV) resp.Value {
	if len(val) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'type' command"}
	}
	key := val[0].Bulk
	kv.SETsMu.RLock()
	if _, ok := kv.SETs[key]; ok {
		kv.SETsMu.RUnlock()
		return resp.Value{Typ: "string", Str: "string"}
	}
	defer kv.SETsMu.RUnlock()
	kv.HashesMu.RLock()
	if _, ok := kv.Hashes[key]; ok {
		kv.HashesMu.RUnlock()
		return resp.Value{Typ: "string", Str: "hash"}
	}
	kv.ListsMu.RLock()
	if _, ok := kv.Lists[key]; ok {
		kv.ListsMu.RUnlock()
		return resp.Value{Typ: "string", Str: "list"}
	}
	return resp.Value{Typ: "string", Str: "none"}
}

func incrementVersion(key string, kv *kv.KV) {
	kv.VersionsMu.Lock()
	kv.Versions[key]++
	kv.VersionsMu.Unlock()
}

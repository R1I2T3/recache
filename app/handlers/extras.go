package handlers

import (
	"strings"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
)

func ping(val []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(val) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}

	return resp.Value{Typ: "string", Str: val[0].Bulk}
}

func echo(val []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(val) == 1 && val[0].Typ == "bulk" {
		return resp.Value{Typ: "string", Str: val[0].Bulk}
	}
	return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'echo' command"}
}

func typeRedis(val []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(val) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'type' command"}
	}
	key := val[0].Bulk
	kv := server.KV
	kv.StringsMu.RLock()
	if _, ok := kv.Strings[key]; ok {
		return resp.Value{Typ: "string", Str: "string"}
	}
	kv.StringsMu.RUnlock()

	kv.HashesMu.RLock()
	if _, ok := kv.Hashes[key]; ok {
		return resp.Value{Typ: "string", Str: "hash"}
	}
	kv.HashesMu.RUnlock()

	kv.ListsMu.RLock()
	if _, ok := kv.Lists[key]; ok {
		return resp.Value{Typ: "string", Str: "list"}
	}
	kv.ListsMu.RUnlock()

	kv.SetsMu.RLock()
	if _, ok := kv.Sets[key]; ok {
		return resp.Value{Typ: "string", Str: "set"}
	}
	kv.SetsMu.RUnlock()

	kv.SortedsMu.RLock()
	if _, ok := kv.Sorteds[key]; ok {
		return resp.Value{Typ: "string", Str: "zset"}
	}
	kv.SortedsMu.RUnlock()

	kv.StreamsMu.RLock()
	if _, ok := kv.Streams[key]; ok {
		return resp.Value{Typ: "string", Str: "stream"}
	}
	kv.StreamsMu.RUnlock()

	return resp.Value{Typ: "string", Str: "none"}
}

func incrementVersion(key string, server *types.Server) {
	server.KV.VersionsMu.Lock()
	server.KV.Versions[key]++
	server.KV.VersionsMu.Unlock()
}

func getConfig(val []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(val) != 2 || val[0].Typ != "bulk" || strings.ToUpper(val[0].Bulk) != "GET" {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'config' command"}
	}
	vals := val[1:]
	result := make([]resp.Value, 0)
	for _, v := range vals {
		switch v.Bulk {
		case "dir":
			result = append(result, resp.Value{Typ: "bulk", Bulk: "dir"}, resp.Value{Typ: "bulk", Bulk: server.Config.Dir})
		case "dbfilename":
			result = append(result, resp.Value{Typ: "bulk", Bulk: "dbFileName"}, resp.Value{Typ: "bulk", Bulk: server.Config.DbFileName})
		}
	}
	return resp.Value{Typ: "array", Array: result}
}

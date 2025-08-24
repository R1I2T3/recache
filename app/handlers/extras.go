package handlers

import (
	"strings"

	"github.com/r1i2t3/go-redis/app/config"
	"github.com/r1i2t3/go-redis/app/resp"
)

func ping(val []resp.Value, server *config.Server) resp.Value {
	if len(val) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}

	return resp.Value{Typ: "string", Str: val[0].Bulk}
}

func echo(val []resp.Value, server *config.Server) resp.Value {
	if len(val) == 2 && val[1].Typ == "bulk" {
		return resp.Value{Typ: "string", Str: val[1].Bulk}
	}
	return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'echo' command"}
}

func typeRedis(val []resp.Value, server *config.Server) resp.Value {
	if len(val) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'type' command"}
	}
	key := val[0].Bulk
	kv := server.KV
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

func incrementVersion(key string, server *config.Server) {
	server.KV.VersionsMu.Lock()
	server.KV.Versions[key]++
	server.KV.VersionsMu.Unlock()
}

func getConfig(val []resp.Value, server *config.Server) resp.Value {
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

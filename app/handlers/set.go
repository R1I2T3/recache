package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
)

func sadd(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'sadd' command"}
	}
	key := args[0].Bulk
	members := args[1:]
	kv := server.KV
	kv.SetsMu.Lock()
	defer kv.SetsMu.Unlock()
	if _, ok := kv.Sets[key]; !ok {
		kv.Sets[key] = make(map[*resp.Value]struct{})
	}
	for _, member := range members {
		kv.Sets[key][&member] = struct{}{}
	}
	return resp.Value{Typ: "integer", Num: (len(members))}
}

func smembers(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'smembers' command"}
	}
	key := args[0].Bulk
	kv := server.KV
	kv.SetsMu.RLock()
	defer kv.SetsMu.RUnlock()
	if members, ok := kv.Sets[key]; ok {
		var result []resp.Value
		for member := range members {
			result = append(result, *member)
		}
		return resp.Value{Typ: "array", Array: result}
	}
	return resp.Value{Typ: "array", Array: []resp.Value{}}
}

func srem(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'srem' command"}
	}
	key := args[0].Bulk
	members := args[1:]
	kv := server.KV
	kv.SetsMu.Lock()
	defer kv.SetsMu.Unlock()
	if _, ok := kv.Sets[key]; !ok {
		return resp.Value{Typ: "integer", Num: 0}
	}
	for _, member := range members {
		delete(kv.Sets[key], &member)
	}
	return resp.Value{Typ: "integer", Num: (len(members))}
}

func scard(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'scard' command"}
	}
	key := args[0].Bulk
	kv := server.KV
	kv.SetsMu.RLock()
	defer kv.SetsMu.RUnlock()
	if members, ok := kv.Sets[key]; ok {
		return resp.Value{Typ: "integer", Num: (len(members))}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

func sunion(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'sunion' command"}
	}
	keys := args
	kv := server.KV
	kv.SetsMu.Lock()
	defer kv.SetsMu.Unlock()
	resultSet := make(map[*resp.Value]struct{})
	for _, key := range keys {
		if members, ok := kv.Sets[key.Bulk]; ok {
			for member := range members {
				resultSet[member] = struct{}{}
			}
		}
	}
	var result []resp.Value
	for member := range resultSet {
		result = append(result, *member)
	}
	return resp.Value{Typ: "array", Array: result}
}

func sinter(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'sinter' command"}
	}
	keys := args
	kv := server.KV
	kv.SetsMu.Lock()
	defer kv.SetsMu.Unlock()
	resultSet := make(map[*resp.Value]struct{})
	for i, key := range keys {
		if members, ok := kv.Sets[key.Bulk]; ok {
			if i == 0 {
				for member := range members {
					resultSet[member] = struct{}{}
				}
			} else {
				for member := range resultSet {
					if _, ok := members[member]; !ok {
						delete(resultSet, member)
					}
				}
			}
		}
	}
	var result []resp.Value
	for member := range resultSet {
		result = append(result, *member)
	}
	return resp.Value{Typ: "array", Array: result}
}

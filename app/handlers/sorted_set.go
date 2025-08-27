package handlers

import (
	"sort"
	"strconv"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
)

func SortKeysByValues(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return m[keys[i]] < m[keys[j]]
	})

	return keys
}

func zadd(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 3 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'ZADD' command"}
	}
	kvStore := server.KV
	key := args[0].Bulk
	score, err := strconv.ParseFloat(args[1].Bulk, 64)

	if err != nil {
		return resp.Value{Typ: "error", Bulk: "ERR invalid score"}
	}
	value := args[2].Bulk
	kvStore.SortedsMu.Lock()
	defer kvStore.SortedsMu.Unlock()
	sorted_set, exists := kvStore.Sorteds[key]
	if !exists {
		sorted_set = make(map[string]float64)
		kvStore.Sorteds[key] = sorted_set
	}
	returns := 1
	if _, exists := sorted_set[value]; exists {
		returns = 0
	}
	sorted_set[value] = score
	incrementVersion(key, server)
	server.IncrementDirty()
	cmd := resp.Value{Typ: "array", Array: append([]resp.Value{{Typ: "bulk", Bulk: "ZADD"}}, args...)}
	server.Propagate(cmd)
	return resp.Value{Typ: "integer", Num: returns}
}

func zscore(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'ZSCORE' command"}
	}
	kvStore := server.KV
	key := args[0].Bulk
	value := args[1].Bulk
	kvStore.SortedsMu.RLock()
	defer kvStore.SortedsMu.RUnlock()
	sorted_set, exists := kvStore.Sorteds[key]
	if !exists {
		return resp.Value{Typ: "null"}
	}
	score, exists := sorted_set[value]
	if !exists {
		return resp.Value{Typ: "null"}
	}
	return resp.Value{Typ: "bulk", Bulk: strconv.FormatFloat(score, 'f', -1, 64)}
}

func zcard(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'ZCARD' command"}
	}
	kvStore := server.KV
	key := args[0].Bulk
	kvStore.SortedsMu.RLock()
	defer kvStore.SortedsMu.RUnlock()
	sorted_set, exists := kvStore.Sorteds[key]
	if !exists {
		return resp.Value{Typ: "integer", Num: 0}
	}
	return resp.Value{Typ: "integer", Num: len(sorted_set)}
}

func zrem(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'ZREM' command"}
	}
	kvStore := server.KV
	key := args[0].Bulk
	value := args[1].Bulk
	kvStore.SortedsMu.Lock()
	defer kvStore.SortedsMu.Unlock()
	sorted_set, exists := kvStore.Sorteds[key]
	if !exists {
		return resp.Value{Typ: "integer", Num: 0}
	}
	if _, exists := sorted_set[value]; exists {
		delete(sorted_set, value)
		return resp.Value{Typ: "integer", Num: 1}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

func zrank(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'ZRANK' command"}
	}
	kvStore := server.KV
	key := args[0].Bulk
	value := args[1].Bulk
	kvStore.SortedsMu.RLock()
	defer kvStore.SortedsMu.RUnlock()
	sorted_set, exists := kvStore.Sorteds[key]
	if !exists {
		return resp.Value{Typ: "null"}
	}
	sorted_keys := SortKeysByValues(sorted_set)
	for i, k := range sorted_keys {
		if k == value {
			return resp.Value{Typ: "integer", Num: i}
		}
	}
	return resp.Value{Typ: "null"}
}

func zrange(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Bulk: "ERR wrong number of arguments for 'ZRANGE' command"}
	}
	kvStore := server.KV
	key := args[0].Bulk
	start, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return resp.Value{Typ: "error", Bulk: "ERR invalid start index"}
	}
	end, err := strconv.Atoi(args[2].Bulk)
	if err != nil {
		return resp.Value{Typ: "error", Bulk: "ERR invalid end index"}
	}
	kvStore.SortedsMu.RLock()
	defer kvStore.SortedsMu.RUnlock()
	sorted_set, exists := kvStore.Sorteds[key]
	if !exists {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}
	sorted_keys := SortKeysByValues(sorted_set)
	if start < 0 {
		start = len(sorted_keys) + start
	}
	if end >= len(sorted_keys) {
		end = len(sorted_keys) - 1
	}
	if end < 0 {
		end = len(sorted_keys) + end
	}
	if start >= len(sorted_keys) || start > end {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}
	result := make([]resp.Value, end-start+1)
	for i := start; i <= end; i++ {
		result[i-start] = resp.Value{Typ: "bulk", Bulk: sorted_keys[i]}
	}
	return resp.Value{Typ: "array", Array: result}
}

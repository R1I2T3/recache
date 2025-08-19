package handlers

import (
	"fmt"
	"strconv"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func rpush(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpush' command"}
	}
	key := args[0].Bulk
	values := args[1:]
	kv.ListsMu.Lock()
	defer kv.ListsMu.Unlock()
	List, exists := kv.Lists[key]
	if !exists {
		List = []resp.Value{}
	}
	for _, v := range values {
		List = append(List, resp.Value{Typ: "bulk", Bulk: v.Bulk})
	}
	kv.Lists[key] = List
	return resp.Value{Typ: "integer", Num: (len(kv.Lists[key]))}
}

func lrange(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lrange' command"}
	}
	key := args[0].Bulk
	fmt.Println(args[1], args[2])
	start, _ := strconv.ParseInt(args[1].Bulk, 10, 64)
	end, _ := strconv.ParseInt(args[2].Bulk, 10, 64)

	kv.ListsMu.Lock()
	defer kv.ListsMu.Unlock()
	List, exists := kv.Lists[key]
	if !exists {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}

	if start < 0 {
		start = int64(len(List)) + int64(start)
	}
	if end < 0 {
		end = int64(len(List)) + int64(end)
	}
	if end >= int64(len(List)) {
		end = int64(len(List)) - 1
	}
	if start > end {
		return resp.Value{Typ: "array", Array: []resp.Value{}}
	}
	return resp.Value{Typ: "array", Array: List[start : end+1]}
}

func lpush(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpush' command"}
	}
	key := args[0].Bulk
	values := args[1:]
	kv.ListsMu.Lock()
	defer kv.ListsMu.Unlock()
	list, exists := kv.Lists[key]
	if !exists {
		list = []resp.Value{}
	}
	newList := make([]resp.Value, len(values)+len(list))
	for i := len(values) - 1; i >= 0; i-- {
		newList[len(values)-1-i] = resp.Value{Typ: "bulk", Bulk: values[i].Bulk}
	}
	copy(newList[len(values):], list)
	kv.Lists[key] = newList
	return resp.Value{Typ: "integer", Num: (len(kv.Lists[key]))}
}

func llen(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'llen' command"}
	}
	key := args[0].Bulk
	kv.ListsMu.Lock()
	defer kv.ListsMu.Unlock()
	list, exists := kv.Lists[key]
	if !exists {
		return resp.Value{Typ: "integer", Num: 0}
	}
	return resp.Value{Typ: "integer", Num: len(list)}
}

func lpop(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpop' command"}
	}
	key := args[0].Bulk
	num_pop := 1
	if len(args) == 2 {
		n, err := strconv.Atoi(args[1].Bulk)
		if err != nil {
			return resp.Value{Typ: "error", Str: "ERR value is not an integer or out of range"}
		}
		if n <= 0 {
			return resp.Value{Typ: "error", Str: "ERR count must be a positive integer"}
		}
		num_pop = n
	}
	kv.ListsMu.Lock()
	defer kv.ListsMu.Unlock()
	list, exists := kv.Lists[key]
	if !exists || len(list) == 0 {
		return resp.Value{Typ: "null"}
	}
	if num_pop == 1 || len(args) == 1 {
		value := list[0]
		kv.Lists[key] = list[1:]
		return resp.Value{Typ: "bulk", Bulk: value.Bulk}
	}
	values := make([]resp.Value, num_pop)
	copy(values, list[:num_pop])
	kv.Lists[key] = list[num_pop:]
	return resp.Value{Typ: "array", Array: values}
}

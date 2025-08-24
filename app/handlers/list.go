package handlers

import (
	"fmt"
	"strconv"
	"time"

	"github.com/r1i2t3/go-redis/app/config"
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func rpush(args []resp.Value, server *config.Server) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpush' command"}
	}
	kv := server.KV
	key := args[0].Bulk
	values := args[1:]
	kv.ListsMu.Lock()
	list := kv.Lists[key]
	for _, v := range values {
		list = append(list, resp.Value{Typ: "bulk", Bulk: v.Bulk})
	}
	kv.Lists[key] = list
	length := len(list)
	kv.ListsMu.Unlock()
	if len(values) > 0 {
		kv.WakeUpClients(key, false)
	}
	incrementVersion(key, server)
	server.IncrementDirty()
	return resp.Value{Typ: "integer", Num: length}
}

func lrange(args []resp.Value, server *config.Server) resp.Value {
	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lrange' command"}
	}
	kv := server.KV
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

func lpush(args []resp.Value, server *config.Server) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpush' command"}
	}
	kv := server.KV
	key := args[0].Bulk
	values := args[1:]
	kv.ListsMu.Lock()
	list := kv.Lists[key]
	for i := len(values) - 1; i >= 0; i-- {
		val := resp.Value{Typ: "bulk", Bulk: values[i].Bulk}
		list = append([]resp.Value{val}, list...)
	}
	kv.Lists[key] = list
	length := len(list)
	kv.ListsMu.Unlock()
	if len(values) > 0 {
		kv.WakeUpClients(key, false)
	}
	incrementVersion(key, server)
	server.IncrementDirty()
	return resp.Value{Typ: "integer", Num: length}
}

func llen(args []resp.Value, server *config.Server) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'llen' command"}
	}
	kv := server.KV
	key := args[0].Bulk
	kv.ListsMu.Lock()
	defer kv.ListsMu.Unlock()
	list, exists := kv.Lists[key]
	if !exists {
		return resp.Value{Typ: "integer", Num: 0}
	}
	return resp.Value{Typ: "integer", Num: len(list)}
}

func lpop(args []resp.Value, server *config.Server) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpop' command"}
	}
	kv := server.KV
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
	incrementVersion(key, server)
	server.IncrementDirty()
	return resp.Value{Typ: "array", Array: values}
}

func rpop(args []resp.Value, server *config.Server) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpop' command"}
	}
	kv := server.KV
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
	if num_pop == 1 {
		value := list[len(list)-1]
		kv.Lists[key] = list[:len(list)-1]
		return resp.Value{Typ: "bulk", Bulk: value.Bulk}
	}
	if num_pop > len(list) {
		num_pop = len(list)
	}
	start := len(list) - num_pop
	values := make([]resp.Value, num_pop)
	for i := 0; i < num_pop; i++ {
		values[i] = list[len(list)-1-i]
	}
	kv.Lists[key] = list[:start]
	incrementVersion(key, server)
	server.IncrementDirty()
	return resp.Value{Typ: "array", Array: values}
}

func blpop(args []resp.Value, server *config.Server) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'blpop' command"}
	}
	kV := server.KV
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[:len(args)-1] {
		keys = append(keys, a.Bulk)
	}
	timeoutInt, err := strconv.Atoi(args[len(args)-1].Bulk)
	if err != nil || timeoutInt < 0 {
		return resp.Value{Typ: "error", Str: "ERR timeout is not a valid integer or out of range"}
	}
	timeout := time.Duration(timeoutInt) * time.Second
RetryPop:
	kV.ListsMu.Lock()
	for _, key := range keys {
		if list, exists := kV.Lists[key]; exists && len(list) > 0 {
			val := list[0]
			kV.Lists[key] = list[1:]
			kV.ListsMu.Unlock()
			incrementVersion(key, server)
			server.IncrementDirty()
			return resp.Value{Typ: "array", Array: []resp.Value{
				{Typ: "bulk", Bulk: key},
				val,
			}}
		}
	}
	kV.ListsMu.Unlock()
	if timeout == 0 {
		return resp.Value{Typ: "null"}
	}
	bc := &kv.BlockedClient{
		Ch:       make(chan bool, 1),
		Keys:     keys,
		Deadline: time.Now().Add(timeout),
	}
	kV.RegisterBlockedClient(bc)
	defer kV.UnregisterBlockedClient(bc)
	wokenUp := <-bc.Ch
	if wokenUp {
		goto RetryPop

	}
	return resp.Value{Typ: "null"}
}

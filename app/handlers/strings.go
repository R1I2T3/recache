package handlers

import (
	"strconv"
	"strings"
	"time"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

func get(val []resp.Value, kv *kv.KV) resp.Value {
	if len(val) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := val[0].Bulk

	kv.SETsMu.RLock()
	value, ok := kv.SETs[key]
	kv.SETsMu.RUnlock()
	if !ok {
		return resp.Value{Typ: "null"}
	}

	if value.Expires > 0 && value.Expires < time.Now().UnixMilli() {
		kv.SETsMu.Lock()
		delete(kv.SETs, key)
		kv.SETsMu.Unlock()
		return resp.Value{Typ: "null"}
	}
	return resp.Value{Typ: "string", Str: value.Str}
}

func set(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	newVal := args[1].Bulk

	var setter string // NX / XX
	var ex, px int    // expire seconds / ms
	keepTTL, get := false, false

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "NX", "XX":
			setter = opt
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX":
			if keepTTL {
				return resp.Value{Typ: "error", Str: "ERR syntax error"}
			}
			if i+1 < len(args) {
				ex, _ = strconv.Atoi(args[i+1].Bulk)
				i++
			} else {
				return resp.Value{Typ: "error", Str: "ERR syntax error"}
			}
		case "PX":
			if keepTTL {
				return resp.Value{Typ: "error", Str: "ERR syntax error"}
			}
			if i+1 < len(args) {
				px, _ = strconv.Atoi(args[i+1].Bulk)
				i++
			} else {
				return resp.Value{Typ: "error", Str: "ERR syntax error"}
			}
		default:
			return resp.Value{Typ: "error", Str: "ERR syntax error"}
		}
	}

	kv.SETsMu.RLock()
	oldVal, exists := kv.SETs[key]
	kv.SETsMu.RUnlock()

	switch setter {
	case "NX":
		if exists {
			return resp.Value{Typ: "null"}
		}
	case "XX":
		if !exists {
			return resp.Value{Typ: "null"}
		}
	}
	expiration := int64(0)
	if keepTTL && exists && oldVal.Expires > 0 {
		expiration = oldVal.Expires
	} else if ex > 0 {
		expiration = time.Now().Add(time.Duration(ex) * time.Second).UnixMilli()
	} else if px > 0 {
		expiration = time.Now().Add(time.Duration(px) * time.Millisecond).UnixMilli()
	}
	insert := resp.Value{Typ: "string", Str: newVal, Expires: expiration}
	kv.SETsMu.Lock()
	kv.SETs[key] = insert
	kv.SETsMu.Unlock()

	if get {
		if exists {
			return oldVal
		}
		return resp.Value{Typ: "null"}
	}

	return resp.Value{Typ: "string", Str: "OK"}
}

func incr(args []resp.Value, kv *kv.KV) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'incr' command"}
	}

	key := args[0].Bulk

	kv.SETsMu.Lock()
	defer kv.SETsMu.Unlock()

	value, exists := kv.SETs[key]
	if !exists || value.Str == "" {
		value = resp.Value{Typ: "string", Str: "0"}
		kv.SETs[key] = value
	}

	num, err := strconv.Atoi(value.Str)
	if err != nil {
		return resp.Value{Typ: "error", Str: "ERR value is not an integer or out of range"}
	}

	num++
	value.Str = strconv.Itoa(num)
	kv.SETs[key] = value

	return resp.Value{Typ: "string", Str: value.Str}
}

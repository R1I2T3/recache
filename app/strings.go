package main

import (
	"strconv"
	"strings"
	"time"
)

var Handlers = map[string]func([]Value, *KV) Value{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

func ping(val []Value, kv *KV) Value {
	if len(val) == 0 {
		return Value{Typ: "string", Str: "PONG"}
	}

	return Value{Typ: "string", Str: val[0].Bulk}
}

func echo(val []Value, kv *KV) Value {
	if len(val) == 2 && val[1].Typ == "bulk" {
		return Value{Typ: "string", Str: val[1].Bulk}
	}
	return Value{Typ: "error", Str: "ERR wrong number of arguments for 'echo' command"}
}

func get(val []Value, kv *KV) Value {
	if len(val) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := val[0].Bulk

	kv.SETsMu.RLock()
	value, ok := kv.SETs[key]
	kv.SETsMu.RUnlock()
	if !ok {
		return Value{Typ: "null"}
	}

	if value.Expires > 0 && value.Expires < time.Now().UnixMilli() {
		kv.SETsMu.Lock()
		delete(kv.SETs, key)
		kv.SETsMu.Unlock()
		return Value{Typ: "null"}
	}
	return Value{Typ: "string", Str: value.Str}
}

func set(args []Value, kv *KV) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
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
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
			if i+1 < len(args) {
				ex, _ = strconv.Atoi(args[i+1].Bulk)
				i++
			} else {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
		case "PX":
			if keepTTL {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
			if i+1 < len(args) {
				px, _ = strconv.Atoi(args[i+1].Bulk)
				i++
			} else {
				return Value{Typ: "error", Str: "ERR syntax error"}
			}
		default:
			return Value{Typ: "error", Str: "ERR syntax error"}
		}
	}

	kv.SETsMu.RLock()
	oldVal, exists := kv.SETs[key]
	kv.SETsMu.RUnlock()

	switch setter {
	case "NX":
		if exists {
			return Value{Typ: "null"}
		}
	case "XX":
		if !exists {
			return Value{Typ: "null"}
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
	insert := Value{Typ: "string", Str: newVal, Expires: expiration}
	kv.SETsMu.Lock()
	kv.SETs[key] = insert
	kv.SETsMu.Unlock()

	if get {
		if exists {
			return oldVal
		}
		return Value{Typ: "null"}
	}

	return Value{Typ: "string", Str: "OK"}
}

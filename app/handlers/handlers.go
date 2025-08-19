package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

var Handlers = map[string]func([]resp.Value, *kv.KV) resp.Value{
	"PING":    ping,
	"ECHO":    echo,
	"SET":     set,
	"GET":     get,
	"RPUSH":   rpush,
	"LRANGE":  lrange,
	"LPUSH":   lpush,
	"LLEN":    llen,
	"LPOP":    lpop,
	"RPOP":    rpop,
	"HSET":    hset,
	"HGET":    hget,
	"HEXISTS": hexists,
	"HDEL":    hdel,
	"HLEN":    hlen,
	"HKEYS":   hkeys,
	"HVALS":   hvals,
	"TYPE":    typeRedis,
	"XADD":    xadd,
}

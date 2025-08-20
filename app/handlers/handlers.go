package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
)

var Handlers = map[string]func([]resp.Value, *kv.KV) resp.Value{
	"PING": ping,
	"ECHO": echo,
	"TYPE": typeRedis,
	// strings command
	"SET": set,
	"GET": get,
	// list commands
	"RPUSH":  rpush,
	"LRANGE": lrange,
	"LPUSH":  lpush,
	"LLEN":   llen,
	"LPOP":   lpop,
	"RPOP":   rpop,
	"BLPOP":  blpop,
	// Hash set command
	"HSET":    hset,
	"HGET":    hget,
	"HEXISTS": hexists,
	"HDEL":    hdel,
	"HLEN":    hlen,
	"HKEYS":   hkeys,
	"HVALS":   hvals,
	// Stream commands
	"XADD":   xadd,
	"XRANGE": xrange,
	"XREAD":  xread,
	// sorted set commands
	"ZADD":   zadd,
	"ZSCORE": zscore,
	"ZCARD":  zcard,
	"ZREM":   zrem,
	"ZRANK":  zrank,
	"ZRANGE": zrange,
}

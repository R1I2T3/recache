package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
)

var Handlers = map[string]func([]resp.Value, *types.Server, *kv.ClientType) resp.Value{
	"PING": ping,
	"ECHO": echo,
	"TYPE": typeRedis,
	// strings command
	"SET":  set,
	"GET":  get,
	"INCR": incr,
	// list commands
	"RPUSH":  rpush,
	"LRANGE": lrange,
	"LPUSH":  lpush,
	"LLEN":   llen,
	"LPOP":   lpop,
	"RPOP":   rpop,
	"BLPOP":  blpop,
	// Set commands
	"SADD":     sadd,
	"SMEMBERS": smembers,
	"SREM":     srem,
	"SCARD":    scard,
	"SUNION":   sunion,
	"SINTER":   sinter,
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
	// rdb
	"BGSAVE": handleBgsave,
	// pubsub
	"PUBLISH":     handlePublish,
	"SUBSCRIBE":   handleSubscribe,
	"UNSUBSCRIBE": handleUnsubscribe,
	// replications
	"INFO":     Info,
	"REPLCONF": REPLCONF,
}

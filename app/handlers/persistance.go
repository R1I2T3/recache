package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
)

func handleBgsave(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if err := rdb.TriggerBackgroundSave(server); err != nil {
		return resp.Value{Typ: "error", Err: err.Error()}
	}
	return resp.Value{Typ: "string", Str: "Background saving started"}
}

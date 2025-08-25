package handlers

import (
	"fmt"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
)

func Info(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'info' command"}
	}
	if args[0].Bulk == "replication" {
		results := make([]resp.Value, 0)
		role := "slave"
		if server.IsMaster {
			role = "master"
		}
		results = append(results, resp.Value{Typ: "bulk", Bulk: fmt.Sprintf("role:%s", role)})
		results = append(results, resp.Value{Typ: "bulk", Bulk: fmt.Sprintf("master_replid:%s", server.ReplicationID)})
		fmt.Println(len(results))
		return resp.Value{Typ: "array", Array: results}
	}
	return resp.Value{Typ: "bulk", Bulk: "Commands not implemented till now"}
}

func handleReplicaOf(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	// Implementation for the REPLICAOF command
	return resp.Value{Typ: "bulk", Bulk: "Commands not implemented til now"}
}

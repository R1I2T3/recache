package handlers

import (
	"fmt"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
	"github.com/r1i2t3/go-redis/app/writer"
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
		results = append(results, resp.Value{Typ: "bulk", Bulk: fmt.Sprintf("master_replioffset:%d\n", server.ReplicationOffset)})
		fmt.Println(len(results))
		return resp.Value{Typ: "array", Array: results}
	}
	return resp.Value{Typ: "bulk", Bulk: "Commands not implemented till now"}
}

func REPLCONF(args []resp.Value, server *types.Server, conn *kv.ClientType) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'replconf' command"}
	}
	if args[0].Bulk != "listening-port" {
		return resp.Value{Typ: "error", Str: "ERR only 'listening-port' option is supported"}
	}
	return resp.Value{Typ: "string", Str: "OK"}
}

func HandlePsync(args []resp.Value, server *types.Server, conn *kv.ClientType) {
	replicaInfo := server.AddReplica(conn.Conn)
	writer := writer.NewWriter(conn.Conn)
	replId := server.ReplicationID
	replOffset := 0
	writer.Write(resp.Value{Typ: "string", Str: fmt.Sprintf("FULLRESYNC %s %d", replId, replOffset)})
	rdbBuffer, err := rdb.SaveToBuffer(server.KV)
	if err != nil {
		fmt.Println("Failed to create RDB snapshot for replica:", err)
		conn.Conn.Close()
		return
	}
	writer.WriteRDB(rdbBuffer)
	server.SetReplicaOnline(replicaInfo)
	fmt.Printf("Replica at %s is now online.\n", conn.Conn.RemoteAddr())

}

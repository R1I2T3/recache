package handlers

import (
	"fmt"
	"strings"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
	"github.com/r1i2t3/go-redis/app/writer"
)

func handleExec(server *types.Server, client *kv.ClientType) resp.Value {
	kV := server.KV
	kV.VersionsMu.Lock()
	for key, watchedVersion := range client.WatchedKeys {
		currentVersion := kV.Versions[key]
		if currentVersion != watchedVersion {
			kV.VersionsMu.Unlock()
			return resp.Value{Typ: "null"}
		}
	}
	kV.VersionsMu.Unlock()
	results := make([]resp.Value, len(client.CommandQueue))
	for i, cmd := range client.CommandQueue {
		command := strings.ToUpper(cmd.Array[0].Bulk)
		args := cmd.Array[1:]
		handler, ok := Handlers[command]
		if !ok {
			results[i] = resp.Value{Typ: "error", Err: "ERR unknown command in queue '" + command + "'"}
			continue
		}

		results[i] = handler(args, server, client)
	}

	return resp.Value{Typ: "array", Array: results}
}

func handleWatch(args []resp.Value, server *types.Server, client *kv.ClientType) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: "error", Err: "ERR wrong number of arguments for 'watch' command"}
	}
	kV := server.KV
	kV.VersionsMu.Lock()
	defer kV.VersionsMu.Unlock()

	for _, keyVal := range args {
		key := keyVal.Bulk
		currentVersion := kV.Versions[key]
		client.WatchedKeys[key] = currentVersion
	}
	return resp.Value{Typ: "string", Str: "OK"}
}

func HandleTransactionCommands(command string, val resp.Value, writer *writer.Writer, client *kv.ClientType, server *types.Server) bool {
	kV := server.KV
	switch command {
	case "EXEC":
		kV.TransactionMu.Lock()
		defer func() {
			client.IsInTransaction = false
			client.CommandQueue = make([]resp.Value, 0)
			client.WatchedKeys = make(map[string]uint64)
			kV.TransactionMu.Unlock()
		}()

		if len(client.CommandQueue) == 0 {
			writer.Write(resp.Value{Typ: "error", Str: "ERR EXEC without commands in queue"})
			return true
		}
		result := handleExec(server, client)
		writer.Write(result)
		return true

	case "DISCARD":
		client.IsInTransaction = false
		client.CommandQueue = make([]resp.Value, 0)
		client.WatchedKeys = make(map[string]uint64)
		writer.Write(resp.Value{Typ: "string", Str: "OK"})
		return true

	default:
		client.CommandQueue = append(client.CommandQueue, val)
		kV.VersionsMu.Lock()
		client.WatchedKeys[val.Array[1].Bulk] = kV.Versions[val.Array[1].Bulk]
		kV.VersionsMu.Unlock()
		writer.Write(resp.Value{Typ: "string", Str: "QUEUED"})
		return true
	}
}

func HandleNonTransactionCommands(command string, args []resp.Value, writer *writer.Writer, client *kv.ClientType, server *types.Server) bool {
	if command == "WATCH" {
		result := handleWatch(args, server, client)
		writer.Write(result)
		return true
	}
	if command == "MULTI" {
		client.IsInTransaction = true
		client.CommandQueue = make([]resp.Value, 0)
		client.WatchedKeys = make(map[string]uint64)
		writer.Write(resp.Value{Typ: "string", Str: "OK"})
		return true
	}
	if command == "EXEC" || command == "DISCARD" {
		writer.Write(resp.Value{Typ: "error", Str: "ERR EXEC without MULTI"})
		return true
	}
	if command == "CONFIG" {
		result := getConfig(args, server, client)
		writer.Write(result)
		return true
	}
	handler, ok := Handlers[command]
	if !ok {
		err := writer.Write(resp.Value{Typ: "string", Str: ""})
		if err != nil {
			fmt.Println("Error writing response:", err)
		}
		return true
	}
	result := handler(args, server, client)
	writer.Write(result)
	return false
}

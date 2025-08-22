package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/r1i2t3/go-redis/app/handlers"
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/writer"
)

func handleExec(writer *writer.Writer, kV *kv.KV, client *kv.ClientType) resp.Value {
	defer func() {
		client.IsInTransaction = false
		client.CommandQueue = make([]resp.Value, 0)
		client.WatchedKeys = make(map[string]uint64)
	}()
	for _, cmd := range client.CommandQueue {
		result := handlers.Handlers[cmd.Array[0].Bulk](cmd.Array[1:], kV)
		if err := writer.Write(result); err != nil {
			return resp.Value{Typ: "error", Str: "ERR error writing response"}
		}
	}
	client.CommandQueue = nil
	return resp.Value{Typ: "simple", Str: "OK"}
}

func isValidRequest(val resp.Value) bool {
	if val.Typ != "array" {
		fmt.Println("Invalid request, expected array")
		return false
	}
	if len(val.Array) == 0 {
		fmt.Println("Invalid request, expected array length > 0")
		return false
	}
	return true
}

func handleTransactionCommands(command string, val resp.Value, writer *writer.Writer, kV *kv.KV, client *kv.ClientType) bool {
	switch command {
	case "EXEC":
		result := handleExec(writer, kV, client)
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
		writer.Write(resp.Value{Typ: "string", Str: "QUEUED"})
		return true
	}
}

func handleNonTransactionCommands(command string, args []resp.Value, writer *writer.Writer, kV *kv.KV, client *kv.ClientType) bool {
	if command == "MULTI" {
		client.IsInTransaction = true
		client.CommandQueue = make([]resp.Value, 0)
		client.WatchedKeys = make(map[string]uint64)
		writer.Write(resp.Value{Typ: "string", Str: "OK"})
		return true
	}
	handler, ok := handlers.Handlers[command]
	if !ok {
		fmt.Println("Invalid command: ", command)
		err := writer.Write(resp.Value{Typ: "string", Str: ""})
		if err != nil {
			fmt.Println("Error writing response:", err)
		}
		return true
	}
	result := handler(args, kV)
	fmt.Println("Result for command:", command, "is", result)
	writer.Write(result)
	return false
}

func handleConnection(conn net.Conn, kV *kv.KV) {
	defer conn.Close()
	parser := resp.NewParser(bufio.NewReader(conn))
	client := &kv.ClientType{
		Conn:            conn,
		IsInTransaction: false,
	}
	kV.Clients[conn.RemoteAddr().String()] = client

	writer := writer.NewWriter(conn)
	for {
		val, err := parser.Parse()
		if err != nil {
			fmt.Println(err)
			return
		}
		if !isValidRequest(val) {
			continue
		}
		command := strings.ToUpper(val.Array[0].Bulk)
		args := val.Array[1:]

		if client.IsInTransaction {
			if handleTransactionCommands(command, val, writer, kV, client) {
				continue
			}
		} else {
			if handleNonTransactionCommands(command, args, writer, kV, client) {
				continue
			}
		}
	}
}

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	kv := kv.NewKv()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, kv)
	}
}

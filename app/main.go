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

func handleConnection(conn net.Conn, kV *kv.KV) {
	defer conn.Close()
	parser := resp.NewParser(bufio.NewReader(conn))
	kV.ClientsMu.Lock()
	client := &kv.ClientType{
		Conn:            conn,
		IsInTransaction: false,
		CommandQueue:    make([]resp.Value, 0),
		WatchedKeys:     make(map[string]uint64),
	}
	kV.Clients[conn.RemoteAddr().String()] = client
	kV.ClientsMu.Unlock()
	writer := writer.NewWriter(conn)
	defer func() {
		kV.ClientsMu.Lock()
		delete(kV.Clients, conn.RemoteAddr().String())
		kV.ClientsMu.Unlock()
	}()
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
			if handlers.HandleTransactionCommands(command, val, writer, kV, client) {
				continue
			}
		} else {
			if handlers.HandleNonTransactionCommands(command, args, writer, kV, client) {
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

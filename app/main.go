package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func handleConnection(conn net.Conn, kv *KV) {

	defer conn.Close()
	parser := NewParser(bufio.NewReader(conn))
	kv.Clients[conn.RemoteAddr().String()] = conn
	for {
		val, err := parser.Parse()
		if err != nil {
			fmt.Println(err)
			return
		}
		if val.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}
		if len(val.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}
		command := strings.ToUpper(val.Array[0].Bulk)
		args := val.Array[1:]
		fmt.Println(command, args)
		writer := NewWriter(conn)
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			err := writer.Write(Value{Typ: "string", Str: ""})
			if err != nil {
				fmt.Println("Error writing response:", err)
				break
			}
			continue
		}
		result := handler(args, kv)
		writer.Write(result)
	}
}

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	kv := NewKv()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, kv)
	}
}

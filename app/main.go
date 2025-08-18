package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var KV sync.Map

func handleConnection(conn net.Conn) {

	defer conn.Close()
	parser := resp.NewParser(bufio.NewReader(conn))
	for {
		val, err := parser.Parse()
		if err != nil {
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
		} else {
			if val.Typ == "array" && len(val.Array) > 0 && val.Array[0].Typ == "bulk" {
				switch val.Array[0].Bulk {
				case "PING":
					conn.Write([]byte("+PONG\r\n"))
				case "ECHO":
					if len(val.Array) == 2 && val.Array[1].Typ == "bulk" {
						conn.Write([]byte(fmt.Sprintf("+%s\r\n", val.Array[1].Bulk)))
					}
				case "SET":
					if len(val.Array) == 3 && val.Array[1].Typ == "bulk" && val.Array[2].Typ == "bulk" {
						KV.Store(val.Array[1].Bulk, val.Array[2].Bulk)
						conn.Write([]byte("+OK\r\n"))
					} else {
						conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					}
				case "GET":
					if len(val.Array) == 2 && val.Array[1].Typ == "bulk" {
						value, ok := KV.Load(val.Array[1].Bulk)
						fmt.Println(value)
						if !ok {
							conn.Write([]byte("+-1\r\n"))
						} else {
							conn.Write([]byte(fmt.Sprintf("+%s\r\n", value)))
						}
					}
				}

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
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

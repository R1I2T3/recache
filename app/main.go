package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

func handleConnection(conn net.Conn) {

	defer conn.Close()
	parser := resp.NewParser(bufio.NewReader(conn))
	for {
		val, err := parser.Parse()
		if err != nil {
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
		} else {
			if val.Typ == "array" {
				if val.Array[0].Typ == "bulk" && val.Array[0].Bulk == "PING" {
					conn.Write([]byte("+PONG" + "\r\n"))
				}
				if val.Array[0].Typ == "bulk" && val.Array[0].Bulk == "ECHO" {
					if len(val.Array) == 2 {
						conn.Write([]byte(fmt.Sprintf("+%s\r\n", val.Array[1].Bulk)))
					}
				}
			}
			conn.Write([]byte("+Data" + "\r\n"))
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

package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Client disconnected:", conn.RemoteAddr())
			return
		}
		fmt.Printf("Message from %v: %s", conn.RemoteAddr(), message)
		conn.Write([]byte("Echo: " + message))
	}
}

func main() {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println("Failed to bind to port 8000")
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

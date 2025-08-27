package replication

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/r1i2t3/go-redis/app/handlers"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
	"github.com/r1i2t3/go-redis/app/writer"
)

func StartReplication(server *types.Server) {
	for {
		masterAddress := net.JoinHostPort(server.MasterHost, strconv.Itoa(server.MasterPort))

		conn, err := net.Dial("tcp", masterAddress)
		if err != nil {
			fmt.Println("Failed to connect to master:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Println("Connected to master:", masterAddress)
		handleSyncWithMaster(conn, server)
		fmt.Println("Disconnected from master, retrying in 2s...")
		time.Sleep(2 * time.Second)
	}
}

func handleSyncWithMaster(conn net.Conn, server *types.Server) {
	defer conn.Close()

	writer := writer.NewWriter(conn)
	parser := resp.NewParser(bufio.NewReader(conn))

	fmt.Println("Performing handshake with master...")
	writer.Write(resp.Value{Typ: "array", Array: []resp.Value{{Typ: "bulk", Bulk: "PING"}}})
	response, err := parser.Parse()
	if err != nil || response.Str != "PONG" {
		fmt.Println("Failed PING handshake with master:", err)
		return
	}
	myPort := strconv.Itoa(server.Config.PORT)
	writer.Write(resp.Value{Typ: "array", Array: []resp.Value{
		{Typ: "bulk", Bulk: "REPLCONF"},
		{Typ: "bulk", Bulk: "listening-port"},
		{Typ: "bulk", Bulk: myPort},
	}})
	parser.Parse()
	writer.Write(resp.Value{Typ: "array", Array: []resp.Value{
		{Typ: "bulk", Bulk: "PSYNC"},
		{Typ: "bulk", Bulk: "?"},
		{Typ: "bulk", Bulk: "-1"},
	}})

	fullResyncResponse, err := parser.Parse()
	if err != nil || !strings.HasPrefix(fullResyncResponse.Str, "FULLRESYNC") {
		fmt.Println("Invalid FULLRESYNC response from master:", err)
		return
	}
	fmt.Println("Handshake successful. Receiving RDB file from master...")
	rdbLength, err := parser.ParseRDBLength()
	if err != nil {
		fmt.Println("Failed to read RDB file length:", err)
		return
	}
	fmt.Println("RDB file length:", rdbLength)
	rdbData := make([]byte, rdbLength)
	if _, err := io.ReadFull(parser.Reader, rdbData); err != nil {
		fmt.Println("Failed to read RDB file data:", err)
		return
	}
	if err := rdb.LoadFromBuffer(rdbData, server.KV); err != nil {
		fmt.Println("Failed to load RDB data from master:", err)
		return
	}
	fmt.Println("RDB file loaded. Entering continuous replication mode.")
	for {
		cmdValue, err := parser.Parse()
		if err != nil {
			fmt.Println("Connection to master lost:", err)
			return
		}
		if cmdValue.Typ != "array" || len(cmdValue.Array) == 0 {
			continue
		}
		command := strings.ToUpper(cmdValue.Array[0].Bulk)
		args := cmdValue.Array[1:]
		if command == "PING" {
			continue
		}

		if command == "REPLCONF" && len(args) > 0 && strings.ToUpper(args[0].Bulk) == "GETACK" {
			writer.Write(resp.Value{Typ: "array", Array: []resp.Value{
				{Typ: "bulk", Bulk: "REPLCONF"},
				{Typ: "bulk", Bulk: "ACK"},
				{Typ: "bulk", Bulk: "0"},
			}})
			continue
		}

		handler, ok := handlers.Handlers[command]
		if ok {
			fmt.Println("Handling command:", command)
			handler(args, server, nil)
		}

	}
}

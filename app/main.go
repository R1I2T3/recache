package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/r1i2t3/go-redis/app/handlers"
	"github.com/r1i2t3/go-redis/app/kv"
	pubsub "github.com/r1i2t3/go-redis/app/pub_sub"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
	"github.com/r1i2t3/go-redis/app/utils"
	"github.com/r1i2t3/go-redis/app/writer"
)

func NewServer(conf *types.Config) *types.Server {
	return &types.Server{
		Config: *conf,
		KV:     kv.NewKv(),
		PS:     pubsub.NewPubSub(),
	}
}

func handleConnection(conn net.Conn, kV *kv.KV, server *types.Server) {
	defer conn.Close()
	parser := resp.NewParser(bufio.NewReader(conn))
	kV.ClientsMu.Lock()
	client := &kv.ClientType{
		Conn:            conn,
		IsInTransaction: false,
		CommandQueue:    make([]resp.Value, 0),
		WatchedKeys:     make(map[string]uint64),
		Subscriptions:   make(map[string]bool),
		MessageChan:     make(chan resp.Value, 16),
	}
	kV.Clients[conn.RemoteAddr().String()] = client
	kV.ClientsMu.Unlock()
	writer := writer.NewWriter(conn)
	defer func() {
		kV.ClientsMu.Lock()
		delete(kV.Clients, conn.RemoteAddr().String())
		kV.ClientsMu.Unlock()
		server.PS.RemoveClient(client)

	}()
	go func() {
		for msg := range client.MessageChan {
			writer.Write(msg)
		}
	}()

	for {
		val, err := parser.Parse()
		if err != nil {
			fmt.Println(err)
			return
		}
		if !utils.IsValidRequest(val) {
			continue
		}
		command := strings.ToUpper(val.Array[0].Bulk)
		args := val.Array[1:]

		if client.IsInTransaction {
			if handlers.HandleTransactionCommands(command, val, writer, client, server) {
				continue
			}
		} else {
			if handlers.HandleNonTransactionCommands(command, args, writer, client, server) {
				continue
			}
		}
	}
}

func ListenAndServer(server *types.Server) {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	kv := server.KV
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn, kv, server)
	}
}

func main() {
	dir := flag.String("dir", "/tmp", "data directory")
	dbfileName := flag.String("dbfilename", "dump.rdb", "database file name")
	flag.Parse()
	config := &types.Config{
		Dir:            *dir,
		DbFileName:     *dbfileName,
		RDBSaveSeconds: 900,
		RDBSaveChanges: 1,
	}
	path := fmt.Sprintf("%s/%s", config.Dir, config.DbFileName)
	server := NewServer(config)
	rdb.Load(path, server.KV)
	go rdb.StartRDBackgroundSave(server)
	ListenAndServer(server)
}

package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/r1i2t3/go-redis/app/config"
	"github.com/r1i2t3/go-redis/app/handlers"
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/writer"
)

func NewServer(conf *config.Config) *config.Server {
	return &config.Server{
		Config: *conf,
		KV:     kv.NewKv(),
	}
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

func handleConnection(conn net.Conn, kV *kv.KV, server *config.Server) {
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

func startRDBackgroundSave(server *config.Server) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		server.StateMutex.Lock()
		shouldSave := time.Since(server.LastSave).Seconds() >= float64(server.Config.RDBSaveSeconds) &&
			server.Dirty >= int64(server.Config.RDBSaveChanges)
		server.StateMutex.Unlock()

		if shouldSave {
			fmt.Println("Save conditions met, starting BGSAVE.")
			handlers.TriggerBackgroundSave(server)
		}
	}
}

func ListenAndServer(server *config.Server) {
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
	config := &config.Config{
		Dir:            *dir,
		DbFileName:     *dbfileName,
		RDBSaveSeconds: 900,
		RDBSaveChanges: 1,
	}
	path := fmt.Sprintf("%s/%s", config.Dir, config.DbFileName)
	server := NewServer(config)
	rdb.Load(path, server.KV)
	go startRDBackgroundSave(server)
	ListenAndServer(server)
}

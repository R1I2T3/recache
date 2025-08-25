package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/r1i2t3/go-redis/app/handlers"
	"github.com/r1i2t3/go-redis/app/kv"
	pubsub "github.com/r1i2t3/go-redis/app/pub_sub"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/replication"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
	"github.com/r1i2t3/go-redis/app/utils"
	"github.com/r1i2t3/go-redis/app/writer"
)

func main() {
	dir := flag.String("dir", "/tmp", "data directory")
	dbfileName := flag.String("dbfilename", "dump.rdb", "database file name")
	portString := flag.String("port", "6379", "server port")
	replicaof := flag.String("replicaof", "127.0.0.1:6379", "Replica host and port")
	MasterHost := ""
	MasterPort := 0
	IsSlave := false
	if *replicaof != "" {
		parts := strings.Split(*replicaof, ":")
		if len(parts) != 2 {
			fmt.Println("Invalid replicaof format")
			os.Exit(1)
		}
		MasterHost = parts[0]
		MasterPort, _ = strconv.Atoi(parts[1])
		IsSlave = true
	}
	flag.Parse()
	port, err := strconv.Atoi(*portString)
	if err != nil {
		fmt.Println("Invalid port number")
		os.Exit(1)
	}
	config := &types.Config{
		Dir:            *dir,
		DbFileName:     *dbfileName,
		RDBSaveSeconds: 900,
		RDBSaveChanges: 1,
		PORT:           port,
	}
	path := fmt.Sprintf("%s/%s", config.Dir, config.DbFileName)
	server := NewServer(config, MasterHost, MasterPort, IsSlave)
	rdb.Load(path, server.KV)
	go rdb.StartRDBackgroundSave(server)
	ListenAndServer(server)
}

func NewServer(conf *types.Config, MasterHost string, MasterPort int, IsSlave bool) *types.Server {
	replication_id, err := utils.GenerateRandomID()
	if err != nil {
		fmt.Println("Failed to create replication_id")
		os.Exit(1)
	}
	return &types.Server{
		Config:            *conf,
		KV:                kv.NewKv(),
		PS:                pubsub.NewPubSub(),
		IsMaster:          !IsSlave,
		IsSlave:           IsSlave,
		MasterHost:        MasterHost,
		MasterPort:        MasterPort,
		ConnectedReplicas: make(map[net.Conn]*replication.ReplicaInfo),
		ReplicasMutex:     sync.RWMutex{},
		ReplicationID:     replication_id,
		ReplicationOffset: 0,
	}
}

func ListenAndServer(server *types.Server) {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Config.PORT))
	if err != nil {
		fmt.Println("Failed to bind to port", server.Config.PORT)
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
		if client.IsSubscribed {
			switch command {
			case "UNSUBSCRIBE", "PING":
				handler, ok := handlers.Handlers[command]
				if ok {
					handler(args, server, client)
				}
			default:
				client.MessageChan <- resp.Value{Typ: "error", Err: "ERR only 'UNSUBSCRIBE' and 'PING' are allowed in this context"}
			}
		}
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

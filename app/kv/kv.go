package kv

import (
	"net"
	"sync"
	"time"

	"github.com/r1i2t3/go-redis/app/resp"
)

type BlockedClient struct {
	Ch      chan resp.Value
	Keys    []string
	Timeout time.Duration
}

var blockedClients = struct {
	sync.Mutex
	m map[string][]*BlockedClient
}{m: make(map[string][]*BlockedClient)}

func (kv *KV) RegisterBlockedClient(bc *BlockedClient) {
	blockedClients.Lock()
	defer blockedClients.Unlock()
	for _, key := range bc.Keys {
		blockedClients.m[key] = append(blockedClients.m[key], bc)
	}
}
func (kv *KV) UnregisterBlockedClient(bc *BlockedClient) {
	blockedClients.Lock()
	defer blockedClients.Unlock()
	for _, key := range bc.Keys {
		if clients, ok := blockedClients.m[key]; ok {
			for i, c := range clients {
				if c == bc {
					blockedClients.m[key] = append(clients[:i], clients[i+1:]...)
					break
				}
			}
			if len(blockedClients.m[key]) == 0 {
				delete(blockedClients.m, key)
			}
		}
	}
}

func (kv *KV) NotifyBlockedClients(key string, val resp.Value) bool {
	blockedClients.Lock()
	defer blockedClients.Unlock()
	if clients, ok := blockedClients.m[key]; ok && len(clients) > 0 {
		bc := clients[0]
		blockedClients.m[key] = clients[1:]
		bc.Ch <- resp.Value{Typ: "array", Array: []resp.Value{
			{Typ: "bulk", Bulk: key},
			val,
		}}
		return true
	}
	return false
}

type StreamEntry struct {
	ID     string
	Fields map[string]resp.Value
}

type Consumer struct {
	Name   string
	LastID string
}

type ConsumerGroup struct {
	Name      string
	Consumers map[string]*Consumer
	Pending   map[string]StreamEntry
}

type Stream struct {
	Entries []StreamEntry
	Groups  map[string]*ConsumerGroup
}
type KV struct {
	SETs   map[string]resp.Value
	SETsMu sync.RWMutex

	Lists   map[string][]resp.Value
	ListsMu sync.RWMutex

	Hashes   map[string]map[string]resp.Value
	HashesMu sync.RWMutex

	Streams   map[string]*Stream
	StreamsMu sync.RWMutex

	Sorteds   map[string]map[string]float64
	SortedsMu sync.RWMutex

	Clients map[string]net.Conn
}

func NewKv() *KV {
	return &KV{
		SETs:    map[string]resp.Value{},
		Hashes:  map[string]map[string]resp.Value{},
		Lists:   map[string][]resp.Value{},
		Streams: map[string]*Stream{},
		Sorteds: map[string]map[string]float64{},
		Clients: map[string]net.Conn{},
	}
}

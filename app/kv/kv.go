package kv

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/r1i2t3/go-redis/app/resp"
)

type BlockedClient struct {
	Ch       chan bool
	Keys     []string
	Deadline time.Time
	Context  map[string]string
}

type StreamId struct {
	Timestamp uint64
	Sequence  uint64
}

type StreamEntry struct {
	ID     StreamId
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
	Entries         []StreamEntry
	Groups          map[string]*ConsumerGroup
	LastGeneratedID []resp.Value
}

type ClientType struct {
	Conn            net.Conn
	IsInTransaction bool
	CommandQueue    []resp.Value
	WatchedKeys     map[string]uint64
	IsSubscribed    bool
	Subscriptions   map[string]bool
	MessageChan     chan resp.Value
}

type KV struct {
	Strings   map[string]resp.Value
	StringsMu sync.RWMutex

	Lists   map[string][]resp.Value
	ListsMu sync.RWMutex

	Hashes   map[string]map[string]resp.Value
	HashesMu sync.RWMutex

	Streams   map[string]*Stream
	StreamsMu sync.RWMutex

	Sets   map[string]map[*resp.Value]struct{}
	SetsMu sync.RWMutex

	Sorteds   map[string]map[string]float64
	SortedsMu sync.RWMutex

	BlockedClientsMu sync.RWMutex
	BlockedClients   map[string][]*BlockedClient

	Versions      map[string]uint64
	VersionsMu    sync.Mutex
	TransactionMu sync.Mutex
	Clients       map[string]*ClientType
	ClientsMu     sync.Mutex
}

func NewKv() *KV {

	return &KV{
		Strings:        map[string]resp.Value{},
		Hashes:         map[string]map[string]resp.Value{},
		Lists:          map[string][]resp.Value{},
		Streams:        map[string]*Stream{},
		Sorteds:        map[string]map[string]float64{},
		Clients:        map[string]*ClientType{},
		BlockedClients: map[string][]*BlockedClient{},
		Sets:           map[string]map[*resp.Value]struct{}{},
		SetsMu:         sync.RWMutex{},
		Versions:       map[string]uint64{},
	}
}

func (kv *KV) RegisterBlockedClient(bc *BlockedClient) {
	kv.BlockedClientsMu.Lock()
	defer kv.BlockedClientsMu.Unlock()
	for _, key := range bc.Keys {
		kv.BlockedClients[key] = append(kv.BlockedClients[key], bc)
	}
}

func (kv *KV) UnregisterBlockedClient(bc *BlockedClient) {
	kv.BlockedClientsMu.Lock()
	defer kv.BlockedClientsMu.Unlock()
	for _, key := range bc.Keys {
		if clients, ok := kv.BlockedClients[key]; ok {
			newClients := make([]*BlockedClient, 0, len(clients)-1)
			for _, client := range clients {
				if client != bc {
					newClients = append(newClients, client)
				}
			}

			if len(newClients) == 0 {
				delete(kv.BlockedClients, key)
			} else {
				kv.BlockedClients[key] = newClients
			}
		}
	}
}

func (kv *KV) WakeUpClients(key string, wakeAll bool) {
	kv.BlockedClientsMu.Lock()
	defer kv.BlockedClientsMu.Unlock()

	clients, ok := kv.BlockedClients[key]
	if !ok || len(clients) == 0 {
		return
	}
	if wakeAll {
		for _, bc := range clients {
			select {
			case bc.Ch <- true:
			default:
			}
		}
	} else {
		bc := clients[0]
		if len(clients) == 1 {
			delete(kv.BlockedClients, key)
		} else {
			kv.BlockedClients[key] = clients[1:]
		}
		bc.Ch <- true
	}
}

func (id StreamId) IsGreaterThan(other StreamId) bool {
	if id.Timestamp >= other.Timestamp {
		return true
	}
	return id.Timestamp == other.Timestamp && id.Sequence >= other.Sequence
}
func (id StreamId) IsSmallerThan(other StreamId) bool {
	if id.Timestamp <= other.Timestamp {
		return true
	}
	return id.Timestamp == other.Timestamp && id.Sequence <= other.Sequence
}

func (id StreamId) ToString() string {
	return fmt.Sprintf("%d-%d", id.Timestamp, id.Sequence)
}

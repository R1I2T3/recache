package kv

import (
	"net"
	"sync"

	"github.com/r1i2t3/go-redis/app/resp"
)

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

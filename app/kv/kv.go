package kv

import (
	"net"
	"sync"

	"github.com/r1i2t3/go-redis/app/resp"
)

type KV struct {
	SETs     map[string]resp.Value
	SETsMu   sync.RWMutex
	Lists    map[string][]resp.Value
	ListsMu  sync.RWMutex
	Hashes   map[string]map[string]resp.Value
	HashesMu sync.RWMutex
	Clients  map[string]net.Conn
}

func NewKv() *KV {
	return &KV{
		SETs:    map[string]resp.Value{},
		Hashes:  map[string]map[string]resp.Value{},
		Lists:   map[string][]resp.Value{},
		Clients: map[string]net.Conn{},
	}
}

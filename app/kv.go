package main

import (
	"net"
	"sync"
)

type KV struct {
	SETs    map[string]Value
	SETsMu  sync.RWMutex
	Clients map[string]net.Conn
}

func NewKv() *KV {
	return &KV{
		SETs:    map[string]Value{},
		Clients: map[string]net.Conn{},
	}
}

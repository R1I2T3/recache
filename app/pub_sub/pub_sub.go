package pubsub

import (
	"sync"

	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/writer"
)

type PubSub struct {
	PubSubMu sync.RWMutex
	Channels map[string]map[*kv.ClientType]bool
}

func NewPubSub() *PubSub {
	return &PubSub{
		Channels: make(map[string]map[*kv.ClientType]bool),
	}
}

func (ps *PubSub) Subscribe(channels []string, client *kv.ClientType) {
	ps.PubSubMu.Lock()
	defer ps.PubSubMu.Unlock()

	for _, channel := range channels {
		if ps.Channels[channel] == nil {
			ps.Channels[channel] = make(map[*kv.ClientType]bool)
		}
		ps.Channels[channel][client] = true
		client.Subscriptions[channel] = true
	}

	client.IsSubscribed = true
}

func (ps *PubSub) Publish(channel, message string) int {
	ps.PubSubMu.RLock()
	defer ps.PubSubMu.RUnlock()

	subscribers, ok := ps.Channels[channel]
	if !ok {
		return 0
	}
	messagePayload := resp.Value{Typ: "array", Array: []resp.Value{
		{Typ: "bulk", Bulk: "message"},
		{Typ: "bulk", Bulk: channel},
		{Typ: "bulk", Bulk: message},
	}}

	for clientConn := range subscribers {
		writer := writer.NewWriter(clientConn.Conn)
		writer.Write(messagePayload)
	}

	return len(subscribers)
}

func (ps *PubSub) RemoveClient(client *kv.ClientType) {
	ps.PubSubMu.Lock()
	defer ps.PubSubMu.Unlock()

	for channel := range client.Subscriptions {
		if subscribers, ok := ps.Channels[channel]; ok {
			delete(subscribers, client)
			if len(subscribers) == 0 {
				delete(ps.Channels, channel)
			}
		}
	}
	close(client.MessageChan)
}

func (ps *PubSub) Unsubscribe(client *kv.ClientType, channel string) {
	ps.PubSubMu.Lock()
	defer ps.PubSubMu.Unlock()
	if subscribers, ok := ps.Channels[channel]; ok {
		delete(subscribers, client)
		if len(subscribers) == 0 {
			delete(ps.Channels, channel)
		}
	}
	delete(client.Subscriptions, channel)

}

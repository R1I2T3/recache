package handlers

import (
	"github.com/r1i2t3/go-redis/app/kv"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/types"
	"github.com/r1i2t3/go-redis/app/writer"
)

func handleSubscribe(args []resp.Value, server *types.Server, client *kv.ClientType) resp.Value {
	if len(args) == 0 {
		writer := writer.NewWriter(client.Conn)
		writer.Write(resp.Value{Typ: "error", Err: "Invalid subscribe command"})
		return resp.Value{Typ: "error", Err: "ERR wrong number of arguments for 'subscribe' command"}
	}

	channelNames := make([]string, len(args))
	for i, arg := range args {
		channelNames[i] = arg.Bulk
	}

	server.PS.Subscribe(channelNames, client)
	writer := writer.NewWriter(client.Conn)
	for i, channel := range channelNames {
		writer.Write(resp.Value{Typ: "array", Array: []resp.Value{
			{Typ: "bulk", Bulk: "subscribe"},
			{Typ: "bulk", Bulk: channel},
			{Typ: "integer", Num: i + 1},
		}})
	}
	return resp.Value{Typ: "string", Str: "OK"}
}

func handlePublish(args []resp.Value, server *types.Server, _ *kv.ClientType) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Err: "ERR wrong number of arguments for 'publish' command"}
	}
	channel := args[0].Bulk
	message := args[1].Bulk

	receiverCount := server.PS.Publish(channel, message)

	return resp.Value{Typ: "integer", Num: receiverCount}
}

func handleUnsubscribe(args []resp.Value, server *types.Server, client *kv.ClientType) resp.Value {
	if !client.IsSubscribed {
		writer := writer.NewWriter(client.Conn)
		writer.Write(resp.Value{Typ: "error", Err: "ERR not subscribed"})
		return resp.Value{Typ: "error", Err: "ERR not subscribed"}
	}

	channelsToUnsubscribe := make([]string, 0)
	if len(args) == 0 {
		for channel := range client.Subscriptions {
			channelsToUnsubscribe = append(channelsToUnsubscribe, channel)
		}
	} else {
		for _, arg := range args {
			channelsToUnsubscribe = append(channelsToUnsubscribe, arg.Bulk)
		}
	}

	writer := writer.NewWriter(client.Conn)
	for _, channel := range channelsToUnsubscribe {
		server.PS.Unsubscribe(client, channel)

		writer.Write(resp.Value{Typ: "array", Array: []resp.Value{
			{Typ: "bulk", Bulk: "unsubscribe"},
			{Typ: "bulk", Bulk: channel},
			{Typ: "integer", Num: len(client.Subscriptions)},
		}})
	}
	if len(client.Subscriptions) == 0 {
		client.IsSubscribed = false
	}
	return resp.Value{Typ: "string", Str: "OK"}
}

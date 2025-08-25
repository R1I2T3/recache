package replication

import "net"

const (
	ReplicaStateWaitingBGSAVE = iota
	ReplicaStateSendingRDB
	ReplicaStateOnline
)

type ReplicaInfo struct {
	Conn   net.Conn
	State  int
	Offset int64
}

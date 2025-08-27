package types

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r1i2t3/go-redis/app/kv"
	pubsub "github.com/r1i2t3/go-redis/app/pub_sub"
	"github.com/r1i2t3/go-redis/app/resp"
	"github.com/r1i2t3/go-redis/app/writer"
)

type Config struct {
	Dir            string
	DbFileName     string
	RDBSaveSeconds int
	RDBSaveChanges int
	PORT           int
}

type ReplicaInfo struct {
	Conn   net.Conn
	State  int
	Offset int64
}
type Server struct {
	Config            Config
	KV                *kv.KV
	Dirty             int64
	LastSave          time.Time
	StateMutex        sync.Mutex
	IsSaving          atomic.Bool
	PS                *pubsub.PubSub
	IsMaster          bool
	IsSlave           bool
	MasterHost        string
	MasterPort        int
	ConnectedReplicas map[net.Conn]*ReplicaInfo
	ReplicasMutex     sync.RWMutex
	ReplicationID     string
	ReplicationOffset int64
}

const (
	ReplicaStateWaitingBGSAVE = iota
	ReplicaStateSendingRDB
	ReplicaStateOnline
)

func (s *Server) IncrementDirty() {
	s.StateMutex.Lock()
	s.Dirty++
	defer s.StateMutex.Unlock()
}

func (s *Server) Propagate(cmd resp.Value) {
	s.ReplicasMutex.RLock()
	defer s.ReplicasMutex.RUnlock()

	for _, replica := range s.ConnectedReplicas {
		// Only send to fully synchronized, online replicas
		if replica.State == ReplicaStateOnline {
			writer := writer.NewWriter(replica.Conn)
			writer.Write(cmd)
		}
	}
}

func (s *Server) AddReplica(conn net.Conn) *ReplicaInfo {
	s.ReplicasMutex.Lock()
	defer s.ReplicasMutex.Unlock()
	replicaInfo := &ReplicaInfo{
		Conn:  conn,
		State: ReplicaStateWaitingBGSAVE,
	}
	s.ConnectedReplicas[conn] = replicaInfo
	return replicaInfo
}

func (s *Server) SetReplicaOnline(replica *ReplicaInfo) {
	s.ReplicasMutex.Lock()
	defer s.ReplicasMutex.Unlock()
	replica.State = ReplicaStateOnline
}

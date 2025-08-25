package types

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r1i2t3/go-redis/app/kv"
	pubsub "github.com/r1i2t3/go-redis/app/pub_sub"
	"github.com/r1i2t3/go-redis/app/replication"
)

type Config struct {
	Dir            string
	DbFileName     string
	RDBSaveSeconds int
	RDBSaveChanges int
	PORT           int
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
	ConnectedReplicas map[net.Conn]*replication.ReplicaInfo
	ReplicasMutex     sync.RWMutex
	ReplicationID     string
	ReplicationOffset int64
}

func (s *Server) IncrementDirty() {
	s.StateMutex.Lock()
	s.Dirty++
	defer s.StateMutex.Unlock()
}

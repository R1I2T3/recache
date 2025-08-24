package types

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/r1i2t3/go-redis/app/kv"
	pubsub "github.com/r1i2t3/go-redis/app/pub_sub"
)

type Config struct {
	Dir            string
	DbFileName     string
	RDBSaveSeconds int
	RDBSaveChanges int
}

type Server struct {
	Config     Config
	KV         *kv.KV
	Dirty      int64
	LastSave   time.Time
	StateMutex sync.Mutex
	IsSaving   atomic.Bool
	PS         *pubsub.PubSub
}

func (s *Server) IncrementDirty() {
	s.StateMutex.Lock()
	s.Dirty++
	defer s.StateMutex.Unlock()
}

package rdb

import (
	"fmt"
	"os"
	"time"

	"github.com/r1i2t3/go-redis/app/types"
)

func StartRDBackgroundSave(server *types.Server) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		server.StateMutex.Lock()
		shouldSave := time.Since(server.LastSave).Seconds() >= float64(server.Config.RDBSaveSeconds) &&
			server.Dirty >= int64(server.Config.RDBSaveChanges)
		server.StateMutex.Unlock()

		if shouldSave {
			fmt.Println("Save conditions met, starting BGSAVE.")
			TriggerBackgroundSave(server)
		}
	}
}

func TriggerBackgroundSave(server *types.Server) error {
	if server.IsSaving.Load() {
		return fmt.Errorf("background save already in progress")
	}

	go func() {
		server.IsSaving.Store(true)
		defer server.IsSaving.Store(false)

		dbfilePath := fmt.Sprintf("%s/%s", server.Config.Dir, server.Config.DbFileName)

		err := Save(dbfilePath+".tmp", server.KV)
		if err != nil {
			fmt.Printf("BGSAVE failed during save: %v\n", err)
			return
		}

		if err := os.Rename(dbfilePath+".tmp", dbfilePath); err != nil {
			fmt.Printf("BGSAVE failed during rename: %v\n", err)
			return
		}

		fmt.Println("Background save successful.")

		server.StateMutex.Lock()
		server.Dirty = 0
		server.LastSave = time.Now()
		server.StateMutex.Unlock()
	}()

	return nil
}

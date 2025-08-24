package handlers

import (
	"fmt"
	"os"
	"time"

	"github.com/r1i2t3/go-redis/app/config"
	"github.com/r1i2t3/go-redis/app/rdb"
	"github.com/r1i2t3/go-redis/app/resp"
)

func handleBgsave(args []resp.Value, server *config.Server) resp.Value {
	if err := TriggerBackgroundSave(server); err != nil {
		return resp.Value{Typ: "error", Err: err.Error()}
	}
	return resp.Value{Typ: "string", Str: "Background saving started"}
}

func TriggerBackgroundSave(server *config.Server) error {
	if server.IsSaving.Load() {
		return fmt.Errorf("background save already in progress")
	}

	go func() {
		server.IsSaving.Store(true)
		defer server.IsSaving.Store(false)

		dbfilePath := fmt.Sprintf("%s/%s", server.Config.Dir, server.Config.DbFileName)

		err := rdb.Save(dbfilePath+".tmp", server.KV)
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

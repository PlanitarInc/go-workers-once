package once

import (
	"fmt"

	"github.com/PlanitarInc/go-workers-once/lua"
	"github.com/garyburd/redigo/redis"
)

var (
	// NOTE: redigo takes care of loading the script for the first time,
	// so we don't have to 'SCRIPT LOAD' it manually.
	updateStateScript *redis.Script
)

func updateJobStatus(conn redis.Conn, key, jid, status string, expire int) (int, error) {
	res, err := redis.Int(updateStateScript.Do(conn, 1, key, jid, status, expire))
	fmt.Printf(" update job state result: res=%d, err=%s\n", res, err)
	return res, err
}

func init() {
	bs, err := lua.Asset("update_status.lua")
	if err != nil {
		panic(err)
	}

	updateStateScript = redis.NewScript(-1, string(bs))
}

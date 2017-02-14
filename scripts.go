package once

import (
	"time"

	"github.com/PlanitarInc/go-workers-once/lua"
	"github.com/garyburd/redigo/redis"
)

var (
	// NOTE: redigo takes care of loading the script for the first time,
	// so we don't have to 'SCRIPT LOAD' it manually.
	updateStateScript *redis.Script
)

func updateJobStatus(
	conn redis.Conn,
	key, jid, status string,
	expire int,
) (int, error) {
	return updateJobStatusAt(conn, key, jid, status, expire, time.Now(), "")
}

func updateJobStatusWithResult(
	conn redis.Conn,
	key, jid, status string,
	expire int,
	result string,
) (int, error) {
	return updateJobStatusAt(conn, key, jid, status, expire, time.Now(), result)
}

func updateJobStatusAt(
	conn redis.Conn,
	key, jid, status string,
	expire int,
	updatedAt time.Time,
	result string,
) (int, error) {
	updatedMs := time2ms(updatedAt)
	res, err := updateStateScript.Do(conn, 1, key,
		jid, status, expire, updatedMs, result)
	return redis.Int(res, err)
}

func init() {
	bs, err := lua.Asset("update_status.lua")
	if err != nil {
		panic(err)
	}

	updateStateScript = redis.NewScript(-1, string(bs))
}

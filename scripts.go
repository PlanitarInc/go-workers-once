package once

import (
	_ "embed"
	"time"

	"github.com/gomodule/redigo/redis"
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

//go:embed update_status.lua
var updateStatusScript string

func init() {
	updateStateScript = redis.NewScript(-1, updateStatusScript)
}

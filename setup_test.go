package once

import (
	"os"

	"github.com/PlanitarInc/go-workers"
)

func setupRedis() {
	redisHost := "localhost"
	redisPort := "6379"

	if val := os.Getenv("REDIS_HOST"); val != "" {
		redisHost = val
	}
	if val := os.Getenv("REDIS_PORT"); val != "" {
		redisPort = val
	}

	workers.Configure(map[string]string{
		"server":    redisHost + ":" + redisPort,
		"process":   "1",
		"database":  "15",
		"pool":      "1",
		"namespace": "testns",
	})
}

func cleanRedis() {
	conn := workers.Config.Pool.Get()
	conn.Do("flushdb")
	conn.Close()
}

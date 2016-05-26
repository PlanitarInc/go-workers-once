package once

import (
	"testing"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
	. "github.com/onsi/gomega"
)

func TestUpdateJobStatus(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:ok"
	val := `{"jid":"1"}`

	{
		res, err := redis.String(conn.Do("SET", key, val))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		res, err := updateJobStatus(conn, key, "1", "BEGALA", 10)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(0))
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(`{"jid":"1","status":"BEGALA"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(10))
	}
}

func TestUpdateJobStatus_NoKey(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:no-key"

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}

	{
		res, err := updateJobStatus(conn, key, "1", "PRIGALA", 10)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-1))
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}
}

func TestUpdateJobStatus_WrongJID(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:wrong-jid"
	val := `{"jid":"3"}`

	{
		res, err := redis.String(conn.Do("SET", key, val))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		res, err := updateJobStatus(conn, key, "–", "POLZALA", 10)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-2))
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(val))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-1))
	}
}

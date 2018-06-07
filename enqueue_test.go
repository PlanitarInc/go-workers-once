package once

import (
	"encoding/json"
	"testing"

	"github.com/PlanitarInc/go-workers"
	"github.com/bitly/go-simplejson"
	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/gomega"
)

func TestGenerateJID(t *testing.T) {
	RegisterTestingT(t)
}

func TestUnsetJobDesc(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:unset-job"
	val := `{"jid":"1"}`

	{
		res, err := redis.String(conn.Do("SET", key, val))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		err := unsetJobDesc(conn, key, "1")
		Ω(err).Should(BeNil())
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}
}

func TestUnsetJobDesc_WrongJid(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:unset-job:wrong-jid"
	val := `{"jid":"1"}`

	{
		res, err := redis.String(conn.Do("SET", key, val))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		err := unsetJobDesc(conn, key, "–")
		Ω(err).Should(BeNil())
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(val))
	}
}

func TestSetNewJobDesc(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:set-job"
	val := `{"jid":"1"}`

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}

	{
		err := setNewJobDesc(conn, key, 100, []byte(val))
		Ω(err).Should(BeNil())
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(val))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(100))
	}
}

func TestSetNewJobDesc_Overrides(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:set-job:override"
	val := `{"jid":"1"}`

	{
		res, err := redis.String(conn.Do("SET", key, "@@@"))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		err := setNewJobDesc(conn, key, 100, []byte(val))
		Ω(err).Should(BeNil())
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(val))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(100))
	}
}

func TestTrySetNewJobDesc(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:try-set-job"
	val := `{"jid":"1"}`

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}

	{
		desc, err := trySetNewDescJob(conn, key, 100, []byte(val))
		Ω(err).Should(BeNil())
		Ω(desc).Should(BeNil())
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(val))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(100))
	}
}

func TestTrySetNewJobDesc_Preexists(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "test-key:try-set-job:preexists"
	val := `{"jid":"1"}`
	oldval := `{"jid":"123"}`

	{
		res, err := redis.String(conn.Do("SET", key, oldval))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		desc, err := trySetNewDescJob(conn, key, 100, []byte(val))
		Ω(err).Should(BeNil())
		Ω(desc).Should(Equal(&JobDesc{Jid: "123"}))
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(oldval))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-1))
	}
}

func TestEnqueueJobDesc(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	desc := NewJobDesc("1", "tor-basic", "typo", nil)
	key := workers.Config.Namespace + "once:q:tor-basic:typo"
	descJson, _ := json.Marshal(desc)

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}

	{
		jid, err := enqueueJobDesc(desc, nil)
		Ω(err).Should(BeNil())
		Ω(jid).Should(Equal("1"))
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(descJson))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(30))
	}

	{
		queue := workers.Config.Namespace + "queue:tor-basic"
		bs, err := redis.Bytes(conn.Do("lpop", queue))
		Ω(err).Should(BeNil())

		msg, err := simplejson.NewJson(bs)
		Ω(err).Should(BeNil())
		Ω(msg).ShouldNot(BeNil())

		tmp, err := msg.Get("x-once").Encode()
		Ω(err).Should(BeNil())
		Ω(tmp).Should(MatchJSON(descJson))

		msg.Del("at")
		msg.Del("enqueued_at")
		msg.Del("x-once")
		tmp, err = msg.Encode()
		Ω(err).Should(BeNil())
		Ω(tmp).Should(
			MatchJSON(`{"jid":"1","queue":"tor-basic","class":"","args":null}`))
	}
}

func TestEnqueueJobDesc_Preexists(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	desc := NewJobDesc("2", "tor-preexists", "typo", nil)
	key := workers.Config.Namespace + "once:q:tor-preexists:typo"
	oldval := `{"jid":"123"}`

	{
		res, err := redis.String(conn.Do("SET", key, oldval))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		jid, err := enqueueJobDesc(desc, nil)
		Ω(err).Should(BeNil())
		Ω(jid).Should(Equal("123"))
	}

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(oldval))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-1))
	}

	{
		queue := workers.Config.Namespace + "queue:tor-preexists"
		n, err := redis.Int(conn.Do("llen", queue))
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(0))
	}
}

func TestEnqueueJobDesc_Overrides(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	desc := NewJobDesc("3", "tor-overrides", "typo", nil)
	key := workers.Config.Namespace + "once:q:tor-overrides:typo"
	oldval := `{"jid":"123"}`
	descJson, _ := json.Marshal(desc)

	{
		res, err := redis.String(conn.Do("SET", key, oldval))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		jid, err := enqueueJobDesc(desc, nil, true)
		Ω(err).Should(BeNil())
		Ω(jid).Should(Equal("3"))
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(descJson))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(30))
	}

	{
		queue := workers.Config.Namespace + "queue:tor-overrides"
		bs, err := redis.Bytes(conn.Do("lpop", queue))
		Ω(err).Should(BeNil())

		msg, err := simplejson.NewJson(bs)
		Ω(err).Should(BeNil())
		Ω(msg).ShouldNot(BeNil())

		tmp, err := msg.Get("x-once").Encode()
		Ω(err).Should(BeNil())
		Ω(tmp).Should(MatchJSON(descJson))

		msg.Del("at")
		msg.Del("enqueued_at")
		msg.Del("x-once")
		tmp, err = msg.Encode()
		Ω(err).Should(BeNil())
		Ω(tmp).Should(
			MatchJSON(`{"jid":"3","queue":"tor-overrides","class":"","args":null}`))
	}
}

func TestEnqueueJobDesc_WithOptions(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	opts := Options{
		EnqueueOptions: workers.EnqueueOptions{
			Retry: true, RetryCount: 2, At: 9.9,
		},
	}
	desc := NewJobDesc("5", "tor-with-options", "typo", &opts)
	key := workers.Config.Namespace + "once:q:tor-with-options:typo"
	descJson, _ := json.Marshal(desc)

	{
		res, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
		Ω(res).Should(BeEmpty())
	}

	{
		jid, err := enqueueJobDesc(desc, nil)
		Ω(err).Should(BeNil())
		Ω(jid).Should(Equal("5"))
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(descJson))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(30))
	}

	{
		queue := workers.Config.Namespace + "queue:tor-with-options"
		bs, err := redis.Bytes(conn.Do("lpop", queue))
		Ω(err).Should(BeNil())

		msg, err := simplejson.NewJson(bs)
		Ω(err).Should(BeNil())
		Ω(msg).ShouldNot(BeNil())

		tmp, err := msg.Get("x-once").Encode()
		Ω(err).Should(BeNil())
		Ω(tmp).Should(MatchJSON(descJson))

		msg.Del("class")
		msg.Del("enqueued_at")
		msg.Del("x-once")
		tmp, err = msg.Encode()
		Ω(err).Should(BeNil())
		Ω(tmp).Should(MatchJSON(`{
			"jid": "5",
			"queue": "tor-with-options",
			"args": null,
			"retry": true,
			"retry_count": 2,
			"at":9.9
		}`))
	}
}

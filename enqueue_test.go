package once

import (
	"encoding/json"
	"testing"

	"github.com/PlanitarInc/go-workers"
	"github.com/bitly/go-simplejson"
	"github.com/gocql/gocql"
	"github.com/gomodule/redigo/redis"
	. "github.com/onsi/gomega"
)

func TestGenerateJID(t *testing.T) {
	RegisterTestingT(t)

	id, err := gocql.ParseUUID(generateJid())
	Ω(err).ShouldNot(HaveOccurred())
	Ω(id).ShouldNot(Equal(gocql.UUID{}))
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

	t.Run("default", func(t *testing.T) {
		reschduleStatuses := []string{StatusInitWaiting, StatusExecuting, StatusRetryWaiting, StatusOK, StatusFailed}

		for _, s := range reschduleStatuses {
			t.Run(s, func(t *testing.T) {
				RegisterTestingT(t)

				key := "test-key:try-set-job:default:preexists:" + s
				val := `{"jid":"1"}`
				oldval := `{"jid":"123","status":"` + s + `"}`

				{
					var d JobDesc
					Ω(json.Unmarshal([]byte(oldval), &d)).Should(BeNil())
					Ω(d.Options).Should(BeNil())
				}

				{
					res, err := redis.String(conn.Do("SET", key, oldval))
					Ω(err).Should(BeNil())
					Ω(res).Should(Equal("OK"))
				}

				{
					desc, err := trySetNewDescJob(conn, key, 100, []byte(val))
					Ω(err).Should(BeNil())
					Ω(desc).Should(Equal(&JobDesc{Jid: "123", Status: s}))
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
			})
		}

		t.Run("badDescriptor", func(t *testing.T) {
			RegisterTestingT(t)

			key := "test-key:try-set-job:preexists:default:bad-descriptor"
			val := `{"jid":"1"}`
			oldval := `"jid":"123"}`

			{
				var d JobDesc
				Ω(json.Unmarshal([]byte(oldval), &d)).ShouldNot(BeNil())
			}

			{
				res, err := redis.String(conn.Do("SET", key, oldval))
				Ω(err).Should(BeNil())
				Ω(res).Should(Equal("OK"))
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
		})
	})

	t.Run("override-started", func(t *testing.T) {
		t.Run("init-waiting", func(t *testing.T) {
			RegisterTestingT(t)

			key := "test-key:try-set-job:preexists:override:init-waiting"
			val := `{"jid":"1"}`
			oldval := `{"jid":"123","status":"init-waiting","options":{"override_started":true}}`

			{
				var d JobDesc
				Ω(json.Unmarshal([]byte(oldval), &d)).Should(BeNil())
				Ω(d.Options).ShouldNot(BeNil())
				Ω(d.Options.OverrideStarted).Should(BeTrue())
			}

			{
				res, err := redis.String(conn.Do("SET", key, oldval))
				Ω(err).Should(BeNil())
				Ω(res).Should(Equal("OK"))
			}

			{
				desc, err := trySetNewDescJob(conn, key, 100, []byte(val))
				Ω(err).Should(BeNil())
				Ω(desc).Should(Equal(&JobDesc{
					Jid:     "123",
					Status:  StatusInitWaiting,
					Options: &Options{OverrideStarted: true},
				}))
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
		})

		reschduleStatuses := []string{StatusExecuting, StatusRetryWaiting, StatusOK, StatusFailed}
		for _, s := range reschduleStatuses {
			t.Run(s, func(t *testing.T) {
				RegisterTestingT(t)

				key := "test-key:try-set-job:preexists:override:" + s
				val := `{"jid":"1"}`
				oldval := `{"jid":"123","status":"` + s + `","options":{"override_started":true}}`

				{
					var d JobDesc
					Ω(json.Unmarshal([]byte(oldval), &d)).Should(BeNil())
					Ω(d.Options).ShouldNot(BeNil())
					Ω(d.Options.OverrideStarted).Should(BeTrue())
				}

				{
					res, err := redis.String(conn.Do("SET", key, oldval))
					Ω(err).Should(BeNil())
					Ω(res).Should(Equal("OK"))
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
			})
		}

		t.Run("badDescriptor", func(t *testing.T) {
			RegisterTestingT(t)

			key := "test-key:try-set-job:preexists:override:bad-descriptor"
			val := `{"jid":"1"}`
			oldval := `"jid":"123"}`

			{
				var d JobDesc
				Ω(json.Unmarshal([]byte(oldval), &d)).ShouldNot(BeNil())
			}

			{
				res, err := redis.String(conn.Do("SET", key, oldval))
				Ω(err).Should(BeNil())
				Ω(res).Should(Equal("OK"))
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
		})
	})
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

func TestEnqueueJobDesc_PreexistsInitWaiting(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	desc := NewJobDesc("2", "tor-preexists-init-waiting", "typo", nil)
	key := workers.Config.Namespace + "once:q:tor-preexists-init-waiting:typo"
	oldval := `{"jid":"123","status":"init-waiting"}`

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
		queue := workers.Config.Namespace + "queue:tor-preexists-init-waiting"
		n, err := redis.Int(conn.Do("llen", queue))
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(0))
	}
}

func TestEnqueueJobDesc_PreexistsExecuting(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	t.Run("default", func(t *testing.T) {
		RegisterTestingT(t)

		desc := NewJobDesc("2", "tor-preexists-default-executing", "typo", nil)
		key := workers.Config.Namespace + "once:q:tor-preexists-default-executing:typo"
		oldval := `{"jid":"123","status":"executing"}`

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
			queue := workers.Config.Namespace + "queue:tor-preexists-default-executing"
			n, err := redis.Int(conn.Do("llen", queue))
			Ω(err).Should(BeNil())
			Ω(n).Should(Equal(0))
		}
	})

	t.Run("override-started", func(t *testing.T) {
		RegisterTestingT(t)

		desc := NewJobDesc("3", "tor-preexists-override-executing", "typo3", nil)
		key := workers.Config.Namespace + "once:q:tor-preexists-override-executing:typo3"
		oldval := `{"jid":"123","status":"executing","options":{"override_started":true}}`
		descJson, _ := json.Marshal(desc)

		{
			res, err := redis.String(conn.Do("SET", key, oldval))
			Ω(err).Should(BeNil())
			Ω(res).Should(Equal("OK"))
		}

		{
			jid, err := enqueueJobDesc(desc, nil)
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
			queue := workers.Config.Namespace + "queue:tor-preexists-override-executing"
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
				MatchJSON(`{"jid":"3","queue":"tor-preexists-override-executing","class":"","args":null}`))
		}
	})
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

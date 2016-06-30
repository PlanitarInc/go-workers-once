package once

import (
	"testing"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
	. "github.com/onsi/gomega"
)

func TestMiddlewareCall(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "1",
		"retry": true,
		"x-once": {
			"job_type": "shlomo"
		}
	}`)
	queue := "tur"
	key := workers.Config.Namespace + "once:q:tur:shlomo"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"1"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"1","status":"ok"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(5))
	}
}

func TestMiddlewareCall_Processing(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "2",
		"retry": true,
		"x-once": {
			"job_type": "moshe"
		}
	}`)
	queue := "tur-processing"
	key := workers.Config.Namespace + "once:q:tur-processing:moshe"

	m := Middleware{}

	processingStartedC := make(chan struct{})
	defer close(processingStartedC)
	processingStopC := make(chan struct{})
	defer close(processingStopC)
	processFunc := func() bool {
		processingStartedC <- struct{}{}
		<-processingStopC
		return true
	}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"2"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		workerDoneC := make(chan struct{})
		defer close(workerDoneC)

		// Start the worker
		go func() {
			ack := m.Call(queue, msg, processFunc)
			Ω(ack).Should(BeTrue())
			workerDoneC <- struct{}{}
		}()

		// Wait for worker to start processing
		<-processingStartedC

		{ // Make sure the job status is set to "executing"
			res, err := redis.Bytes(conn.Do("GET", key))
			Ω(err).Should(BeNil())
			Ω(res).Should(MatchJSON(`{"jid":"2","status":"executing"}`))
		}
		{
			res, err := redis.Int(conn.Do("TTL", key))
			Ω(err).Should(BeNil())
			Ω(res).Should(Equal(90))
		}

		// Release the worker and wait for its completion
		processingStopC <- struct{}{}
		<-workerDoneC
	}
}

func TestMiddlewareCall_Failed(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "3",
		"retry": true,
		"x-once": {
			"job_type": "rahamim"
		}
	}`)
	queue := "tur-failed"
	key := workers.Config.Namespace + "once:q:tur-failed:rahamim"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"3"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		Ω(func() {
			_ = m.Call(queue, msg, panicNext)
		}).Should(Panic())
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"3","status":"failed"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(5))
	}
}

func TestMiddlewareCall_Retrying(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "4",
		"retry": true,
		"x-once": {
			"job_type": "yair"
		}
	}`)
	queue := "tur-retrying"
	key := workers.Config.Namespace + "once:q:tur-retrying:yair"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"4"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		Ω(func() {
			_ = m.Call(queue, msg, func() bool {
				rm := workers.MiddlewareRetry{}
				return rm.Call(queue, msg, panicNext)
			})
		}).Should(Panic())
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"4","status":"retry-waiting"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(60))
	}
}

func TestMiddlewareCall_NoKey(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "5",
		"retry": true,
		"x-once": {
			"job_type": "dudu"
		}
	}`)
	queue := "tur-no-key"
	key := workers.Config.Namespace + "once:q:tur-no-key:dudu"

	m := Middleware{}

	{
		_, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
	}

	{
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
	}

	{
		_, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
	}
}

func TestMiddlewareCall_WrongJid(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "6",
		"retry": true,
		"x-once": {
			"job_type": "kfir"
		}
	}`)
	queue := "tur-wrong-jid"
	key := workers.Config.Namespace + "once:q:tur-wrong-jid:kfir"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"123"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"123"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-1))
	}
}

func TestMiddlewareCall_NoXOnce(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	// there is no `x-once` field, basically meaning the job was not
	// enqueued using the right `Enqueue`; hence the middleware
	// should just execute the job and do nothing other.
	msg, _ := workers.NewMsg(`{
		"jid": "6",
		"retry": true
	}`)
	queue := "tur-no-x-once"
	key := workers.Config.Namespace + "once:q:tur-no-x-once:"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"6"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
	}

	{
		// Although the JID matches, the key should be ignored
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"6"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(-1))
	}
}

func TestMiddlewareCall_NamespacedQueue(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "1",
		"retry": true,
		"x-once": {
			"job_type": "dolev"
		}
	}`)
	queue := "tur-namespaced-queue"
	key := workers.Config.Namespace + "once:q:tur-namespaced-queue:dolev"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"1"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		ack := m.Call(workers.Config.Namespace+queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"1","status":"ok"}`))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(5))
	}
}

func noopNext() bool {
	return true
}

func panicNext() bool {
	panic("Allahu Akbar!")
}

package once

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/gomodule/redigo/redis"
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
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1))
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())

		resObj := map[string]interface{}{}
		err = json.Unmarshal(res, &resObj)
		Ω(err).Should(BeNil())

		Ω(resObj["jid"]).Should(Equal("1"))
		Ω(resObj["status"]).Should(Equal("ok"))

		nowMs := time.Now().UnixNano() / 1e6
		Ω(resObj["updated_ms"]).Should(BeBetween(nowMs-100, nowMs+100))
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

			resObj := map[string]interface{}{}
			err = json.Unmarshal(res, &resObj)
			Ω(err).Should(BeNil())

			Ω(resObj["jid"]).Should(Equal("2"))
			Ω(resObj["status"]).Should(Equal("executing"))

			nowMs := time.Now().UnixNano() / 1e6
			Ω(resObj["updated_ms"]).Should(BeBetween(nowMs-100, nowMs+100))
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

		resObj := map[string]interface{}{}
		err = json.Unmarshal(res, &resObj)
		Ω(err).Should(BeNil())

		Ω(resObj["jid"]).Should(Equal("3"))
		Ω(resObj["status"]).Should(Equal("failed"))

		nowMs := time.Now().UnixNano() / 1e6
		Ω(resObj["updated_ms"]).Should(BeBetween(nowMs-100, nowMs+100))
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

		resObj := map[string]interface{}{}
		err = json.Unmarshal(res, &resObj)
		Ω(err).Should(BeNil())

		Ω(resObj["jid"]).Should(Equal("4"))
		Ω(resObj["status"]).Should(Equal("retry-waiting"))

		nowMs := time.Now().UnixNano() / 1e6
		Ω(resObj["updated_ms"]).Should(BeBetween(nowMs-100, nowMs+100))
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
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1))
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
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1))
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
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1))
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
		"jid": "7",
		"retry": true,
		"x-once": {
			"job_type": "dolev"
		}
	}`)
	queue := "tur-namespaced-queue"
	key := workers.Config.Namespace + "once:q:tur-namespaced-queue:dolev"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"7"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		counter, noopNext := getCountableCb()
		ack := m.Call(workers.Config.Namespace+queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1))
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())

		resObj := map[string]interface{}{}
		err = json.Unmarshal(res, &resObj)
		Ω(err).Should(BeNil())

		Ω(resObj["jid"]).Should(Equal("7"))
		Ω(resObj["status"]).Should(Equal("ok"))

		nowMs := time.Now().UnixNano() / 1e6
		Ω(resObj["updated_ms"]).Should(BeBetween(nowMs-100, nowMs+100))
	}

	{
		res, err := redis.Int(conn.Do("TTL", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(5))
	}
}

func TestMiddlewareCall_AtMostOnce(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "7",
		"retry": true,
		"x-once": {
			"job_type": "shabtai",
			"options": {
				"at_most_once": true
			}
		}
	}`)
	queue := "tur-once-at-most"
	key := workers.Config.Namespace + "once:q:tur-once-at-most:shabtai"

	m := Middleware{}

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"123"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(0))
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

func getCountableCb() (*int, func() bool) {
	callCounter := 0
	cb := func() bool {
		callCounter++
		return true
	}

	return &callCounter, cb
}

func panicNext() bool {
	panic("Allahu Akbar!")
}

// An at-most-once job whose descriptor expired while it was executing must
// still run when go-workers reschedules it. Before the retry branch in
// Middleware.Call this was silently dropped: the key was gone (n == -1), which
// the middleware treated the same as "superseded", so next() was never called.
//
// Note what a missing key means for a lock -- nothing else holds it, so the
// retry is free to proceed.
func TestMiddlewareCall_AtMostOnce_ExpiredKeyOnRetry(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "9",
		"retry": true,
		"retry_count": 1,
		"x-once": {
			"jid": "9",
			"queue": "tur-expired-retry",
			"job_type": "gershon",
			"options": {
				"at_most_once": true,
				"exec_wait": 120
			}
		}
	}`)
	queue := "tur-expired-retry"
	key := workers.Config.Namespace + "once:q:tur-expired-retry:gershon"

	m := Middleware{}

	// The descriptor expired during the previous attempt. Delete it rather than
	// asserting it is absent: other tests in this package leak keys from
	// background pub/sub goroutines, so an emptiness assertion here is flaky.
	{
		_, err := conn.Do("DEL", key)
		Ω(err).Should(BeNil())
	}

	{
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1), "the retry must actually run")
	}

	// The descriptor was reclaimed, so waiters can observe the outcome again.
	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())

		desc := JobDesc{}
		Ω(json.Unmarshal(res, &desc)).Should(BeNil())
		Ω(desc.Jid).Should(Equal("9"))
		Ω(desc.JobType).Should(Equal("gershon"))
		Ω(desc.Status).Should(Equal(StatusOK), "next() succeeded, so the run is recorded")
	}
}

// The FIRST retry is the case the fix exists for, and go-workers labels it
// retry_count=0 (incrementRetry sets 0, not 1, on the first failure). A guard
// of `retryCount > 0` passes every other test in this file and still drops
// this one, so it gets its own test.
func TestMiddlewareCall_AtMostOnce_ExpiredKeyOnFirstRetry(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "9",
		"retry": true,
		"retry_count": 0,
		"x-once": {
			"jid": "9",
			"queue": "tur-expired-retry0",
			"job_type": "gershon",
			"options": {
				"at_most_once": true,
				"exec_wait": 120
			}
		}
	}`)
	queue := "tur-expired-retry0"
	key := workers.Config.Namespace + "once:q:tur-expired-retry0:gershon"

	m := Middleware{}

	{
		_, err := conn.Do("DEL", key)
		Ω(err).Should(BeNil())
	}

	{
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(1), "the first retry must actually run")
	}

	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())

		desc := JobDesc{}
		Ω(json.Unmarshal(res, &desc)).Should(BeNil())
		Ω(desc.Jid).Should(Equal("9"))
		Ω(desc.Status).Should(Equal(StatusOK))
	}
}

// The same expired-key situation on a FIRST attempt keeps the old behaviour:
// nothing rescheduled this job, so it is genuinely lost and must be dropped.
func TestMiddlewareCall_AtMostOnce_ExpiredKeyFirstAttempt(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "10",
		"retry": true,
		"x-once": {
			"jid": "10",
			"queue": "tur-expired-first",
			"job_type": "yaffa",
			"options": {
				"at_most_once": true
			}
		}
	}`)
	queue := "tur-expired-first"
	key := workers.Config.Namespace + "once:q:tur-expired-first:yaffa"

	m := Middleware{}

	{
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(0), "a first attempt with no descriptor is still dropped")
	}

	{
		_, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil), "nothing should have been reclaimed")
	}
}

// A retry must NOT resurrect itself over a newer job of the same type. This is
// the regression the SET ... NX guards against: without it the retry would
// overwrite the newer job's descriptor and both would run.
func TestMiddlewareCall_AtMostOnce_RetryDoesNotStompNewerJob(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	msg, _ := workers.NewMsg(`{
		"jid": "11",
		"retry": true,
		"retry_count": 2,
		"x-once": {
			"jid": "11",
			"queue": "tur-retry-stomp",
			"job_type": "shlomo",
			"options": {
				"at_most_once": true
			}
		}
	}`)
	queue := "tur-retry-stomp"
	key := workers.Config.Namespace + "once:q:tur-retry-stomp:shlomo"

	m := Middleware{}

	// A newer job of the same type already owns the descriptor.
	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"999"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		counter, noopNext := getCountableCb()
		ack := m.Call(queue, msg, noopNext)
		Ω(ack).Should(BeTrue())
		Ω(*counter).Should(Equal(0), "the superseded retry must not run")
	}

	// The newer job's descriptor is untouched.
	{
		res, err := redis.Bytes(conn.Do("GET", key))
		Ω(err).Should(BeNil())
		Ω(res).Should(MatchJSON(`{"jid":"999"}`))
	}
}

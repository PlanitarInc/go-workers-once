package once

import (
	"net"
	"testing"
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
	. "github.com/onsi/gomega"
)

func TestGetDescriptor(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "testdb:get-desc-1"

	{
		res, err := redis.String(conn.Do("SET", key, `{
			"jid": "1",
			"queue": "q1"
		}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		desc, err := getDescriptor(conn, key)
		Ω(err).Should(BeNil())
		Ω(desc).Should(Equal(&JobDesc{
			Jid:   "1",
			Queue: "q1",
		}))
	}
}

func TestGetDescriptor_NoKey(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := "testdb:get-desc-2"

	{
		_, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
	}

	{
		desc, err := getDescriptor(conn, key)
		Ω(err).Should(Equal(NoMatchingJobsErr))
		Ω(desc).Should(BeNil())
	}
}

func TestWaitForJobType(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	queue := "wait-1"
	jobType := "email"
	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"1", "status":"ok"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		desc, err := WaitForJobType(queue, jobType)
		Ω(err).Should(BeNil())
		Ω(desc).Should(Equal(&JobDesc{
			Jid:    "1",
			Status: StatusOK,
		}))
	}
}

func TestWaitForJobType_Failed(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	queue := "wait-2"
	jobType := "email"
	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"1", "status":"failed"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	{
		desc, err := WaitForJobType(queue, jobType)
		Ω(err).Should(BeNil())
		Ω(desc).Should(Equal(&JobDesc{
			Jid:    "1",
			Status: StatusFailed,
		}))
	}
}

func TestWaitForJobType_WaitUntilDone(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	queue := "wait-3"
	jobType := "email"
	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	{
		res, err := redis.String(conn.Do("SET", key, `{"jid":"1", "status":"retry-waiting"}`))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))
	}

	startWaitingC := make(chan struct{})
	doneWaitingC := make(chan struct{})

	// Start waiting goroutine
	go func() {
		go func() {
			time.Sleep(10 * time.Millisecond)
			startWaitingC <- struct{}{}
		}()

		desc, err := WaitForJobType(queue, jobType)
		Ω(startWaitingC).Should(BeClosed())
		Ω(err).Should(BeNil())
		Ω(desc).Should(Equal(&JobDesc{
			Jid:    "1",
			Status: StatusOK,
		}))
		close(doneWaitingC)
	}()

	// Wait until waiting go routing is ready
	<-startWaitingC
	time.Sleep(10 * time.Millisecond)
	close(startWaitingC)

	{
		jsonVal := []byte(`{"jid":"1", "status":"ok"}`)

		res, err := redis.String(conn.Do("SET", key, jsonVal))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))

		nreaders, err := redis.Int(conn.Do("PUBLISH", key, jsonVal))
		Ω(err).Should(BeNil())
		Ω(nreaders).Should(Equal(1))
	}

	<-doneWaitingC
	Ω(doneWaitingC).Should(BeClosed())
}

func TestWaitForJobType_StopIfEmpty(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	queue := "wait-4"
	jobType := "email"
	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	{
		_, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
	}

	{
		desc, err := WaitForJobType(queue, jobType, WaitOptions{
			StopIfEmpty: true,
		})
		Ω(err).Should(Equal(NoMatchingJobsErr))
		Ω(desc).Should(BeNil())
	}
}

func TestWaitForJobType_WaitUntilAddedAndDone(t *testing.T) {
	RegisterTestingT(t)

	setupRedis()
	defer cleanRedis()

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	queue := "wait-5"
	jobType := "email"
	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	{
		_, err := redis.String(conn.Do("GET", key))
		Ω(err).Should(Equal(redis.ErrNil))
	}

	startWaitingC := make(chan struct{})
	doneWaitingC := make(chan struct{})

	// Start waiting goroutine
	go func() {
		go func() {
			time.Sleep(10 * time.Millisecond)
			startWaitingC <- struct{}{}
		}()

		desc, err := WaitForJobType(queue, jobType)
		Ω(startWaitingC).Should(BeClosed())
		Ω(err).Should(BeNil())
		Ω(desc).Should(Equal(&JobDesc{
			Jid:    "1",
			Status: StatusFailed,
		}))
		close(doneWaitingC)
	}()

	// Wait until waiting go routing is ready
	<-startWaitingC
	time.Sleep(10 * time.Millisecond)
	close(startWaitingC)

	{
		jsonVal := []byte(`{"jid":"1", "status":"failed"}`)

		res, err := redis.String(conn.Do("SET", key, jsonVal))
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal("OK"))

		nreaders, err := redis.Int(conn.Do("PUBLISH", key, jsonVal))
		Ω(err).Should(BeNil())
		Ω(nreaders).Should(Equal(1))
	}

	<-doneWaitingC
	Ω(doneWaitingC).Should(BeClosed())
}

func TestWaitForJobType_NoRedisConnection(t *testing.T) {
	RegisterTestingT(t)

	workers.Configure(map[string]string{
		"server":    "127.0.0.1:1",
		"process":   "1",
		"database":  "15",
		"pool":      "1",
		"namespace": "testns",
	})

	conn := workers.Config.Pool.Get()
	defer conn.Close()

	queue := "wait-6"
	jobType := "email"

	desc, err := WaitForJobType(queue, jobType)
	Ω(err).ShouldNot(BeNil())
	netErr, ok := err.(*net.OpError)
	Ω(ok).Should(BeTrue())
	Ω(netErr).ShouldNot(BeNil())
	Ω(netErr.Op).Should(Equal("dial"))
	Ω(netErr.Net).Should(Equal("tcp"))
	Ω(desc).Should(BeNil())
}

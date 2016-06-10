package once

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
)

var (
	NoMatchingJobsErr = errors.New("no matching jobs found")
	AbortedErr        = errors.New("aborted")
	TimeoutErr        = errors.New("timeout")
)

type WaitOptions struct {
	StopIfEmpty bool
	Timeout     time.Duration
}

func WaitForJobType(queue, jobType string, options ...WaitOptions) (*JobDesc, error) {
	opts := WaitOptions{}
	if len(options) > 0 {
		opts = options[0]
	}

	if opts.Timeout == 0 {
		opts.Timeout = time.Hour
	}

	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	tracker := jobTracker{
		Key:     key,
		Options: opts,
	}
	desc, err := tracker.Wait()

	return desc, err
}

func GetDesc(queue, jobType string) (*JobDesc, error) {
	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	return getDescriptor(conn, key)
}

func getDescriptor(conn redis.Conn, key string) (*JobDesc, error) {
	descJson, err := redis.Bytes(conn.Do("GET", key))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	// Job is not in the queue
	if err == redis.ErrNil {
		return nil, NoMatchingJobsErr
	}

	desc := JobDesc{}
	err = json.Unmarshal(descJson, &desc)
	if err != nil {
		return nil, err
	}

	return &desc, nil
}

type jobTracker struct {
	Conn       redis.Conn
	PubSubConn *redis.PubSubConn
	Key        string
	Options    WaitOptions

	aborted chan struct{}
}

type asyncResut struct {
	JobDesc *JobDesc
	Error   error
}

func (t *jobTracker) Wait() (*JobDesc, error) {
	t.Conn = workers.Config.Pool.Get()
	defer t.Conn.Close()

	pubsubconn := workers.Config.Pool.Get()
	t.PubSubConn = &redis.PubSubConn{pubsubconn}
	defer pubsubconn.Close()

	// 2 is more than enough
	result := make(chan *asyncResut, 2)
	defer close(result)

	wg := sync.WaitGroup{}
	desc, err := t.waitForCompletion(&wg, result)
	wg.Wait()

	return desc, err
}

func (t *jobTracker) waitForCompletion(
	wg *sync.WaitGroup,
	result chan *asyncResut,
) (*JobDesc, error) {
	var desc *JobDesc
	var err error

	t.aborted = make(chan struct{})
	defer close(t.aborted)

	// The channel is used to sync the subscribe and getOnce goroutines.
	subscribed := make(chan struct{})
	defer close(subscribed)

	go t.subscribeWait(wg, result, subscribed)
	defer t.unsubscribeWait()

	desc, err = t.getIfDone(subscribed)
	if desc != nil || err != nil {
		return desc, err
	}

	select {
	case res := <-result:
		desc = res.JobDesc
		err = res.Error

	case <-t.aborted:
		err = AbortedErr

	case <-time.After(t.Options.Timeout):
		err = TimeoutErr
	}

	return desc, err
}

func (t jobTracker) Stop() {
	t.aborted <- struct{}{}
}

func (t jobTracker) subscribeWait(
	wg *sync.WaitGroup,
	result chan<- *asyncResut,
	subscribed chan<- struct{},
) {
	wg.Add(1)
	defer wg.Done()

	if err := t.PubSubConn.Subscribe(t.Key); err != nil {
		result <- &asyncResut{nil, err}
		subscribed <- struct{}{}
		return
	}

	subscribed <- struct{}{}

	for {
		switch v := t.PubSubConn.Receive().(type) {
		case redis.Message:

			desc := JobDesc{}
			err := json.Unmarshal(v.Data, &desc)
			if err != nil {
				result <- &asyncResut{nil, err}
				return
			}

			// XXX should always be done at this point
			if desc.IsDone() {
				result <- &asyncResut{&desc, err}
				return
			}

		case redis.Subscription:
			if v.Count == 0 {
				return
			}

		case error:
			result <- &asyncResut{nil, v}
			return
		}
	}
}

func (t jobTracker) unsubscribeWait() {
	t.PubSubConn.Unsubscribe(t.Key)
}

func (t jobTracker) getIfDone(subscribed <-chan struct{}) (*JobDesc, error) {
	// Wait until we're subscribed to the job updates
	_, ok := <-subscribed
	if !ok {
		return nil, nil
	}

	desc, err := getDescriptor(t.Conn, t.Key)
	if err != nil && err != NoMatchingJobsErr {
		return nil, err
	}

	if err == NoMatchingJobsErr && t.Options.StopIfEmpty {
		return nil, NoMatchingJobsErr
	}

	if desc != nil && desc.IsDone() {
		return desc, nil
	}

	return nil, nil
}

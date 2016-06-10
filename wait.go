package once

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
)

type WaitOptions struct {
	StopIfEmpty bool
}

func WaitForJobType(queue, jobType string, options ...WaitOptions) (*JobDesc, error) {
	opts := WaitOptions{}
	if len(options) > 0 {
		opts = options[0]
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
		return nil, nil
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

func (t *jobTracker) Wait() (*JobDesc, error) {
	t.Conn = workers.Config.Pool.Get()
	defer t.Conn.Close()

	pubsubconn := workers.Config.Pool.Get()
	t.PubSubConn = &redis.PubSubConn{pubsubconn}
	defer pubsubconn.Close()

	wg := sync.WaitGroup{}
	desc, err := t.waitForCompletion(&wg)
	wg.Wait()

	return desc, err
}

type asyncResut struct {
	JobDesc *JobDesc
	Error   error
}

func (t *jobTracker) waitForCompletion(wg *sync.WaitGroup) (*JobDesc, error) {
	var desc *JobDesc
	var err error

	t.aborted = make(chan struct{})
	defer close(t.aborted)

	result := make(chan *asyncResut)
	defer close(result)

	// The channel is used to sync the subscribe and getOnce goroutines.
	subscribed := make(chan struct{})
	defer close(subscribed)

	go t.subscribeWait(wg, result, subscribed)
	defer t.unsubscribeWait()

	go t.getIfDone(wg, result, subscribed)

	select {
	case res := <-result:
		desc = res.JobDesc
		err = res.Error

	case <-t.aborted:
		err = fmt.Errorf("Aborted")
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

func (t jobTracker) getIfDone(
	wg *sync.WaitGroup,
	result chan<- *asyncResut,
	subscribed <-chan struct{},
) {
	wg.Add(1)
	defer wg.Done()

	// Wait until we're subscribed to the job updates
	_, ok := <-subscribed
	if !ok {
		return
	}

	desc, err := getDescriptor(t.Conn, t.Key)
	if err != nil {
		result <- &asyncResut{nil, err}
		return
	}

	if desc == nil && t.Options.StopIfEmpty {
		result <- &asyncResut{nil, nil}
		return
	}

	if desc != nil && desc.IsDone() {
		result <- &asyncResut{desc, nil}
		return
	}
}

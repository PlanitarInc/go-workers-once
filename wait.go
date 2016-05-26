package once

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
)

type WaitOptions struct {
	StopIfEmpty bool
}

func WaitForJobType(queue, jobType string, options ...WaitOptions) (*JobDesc, error) {
	conn := workers.Config.Pool.Get()
	defer conn.Close()

	opts := WaitOptions{}
	if len(options) > 0 {
		opts = options[0]
	}

	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType

	// XXX Busy wait. Reimplement using Redis pub/sub functionality.
	for {
		desc, err := getDescriptor(conn, key)
		if err != nil {
			return nil, err
		}

		fmt.Printf("got: opts=%v, desc=%v, err=%s\n", opts, desc, err)
		if desc == nil && opts.StopIfEmpty {
			return nil, nil
		}

		if desc != nil && desc.IsDone() {
			return desc, nil
		}

		time.Sleep(100 * time.Millisecond)
	}
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

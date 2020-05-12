package once

import (
	"encoding/json"
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/gocql/gocql"
	"github.com/gomodule/redigo/redis"
)

// Enqueue schedules the given task to the given queue, if no task of the same
// type is already scheduled to run yet. If there is a task of the same type but
// it is executing or was executed at least once (waiting for a retry, failed or
// succeeded), a new task is scheduled anyway basically overriding the existing
// one. It should not matter since the tasks are of the same type and hence
// should be identical.
func Enqueue(
	queue, jobType string,
	args interface{},
	opts *Options,
) (string, error) {
	return enqueueJobDesc(
		NewJobDesc(generateJid(), queue, jobType, opts),
		args,
	)
}

// Enqueue schedules the given task to the given queue with the given delay, if
// no task of the same type is already scheduled to run yet. If there is a task
// of the same type but it is executing or was executed at least once (waiting
// for a retry, failed or succeeded), a new task is scheduled anyway basically
// overriding the existing one. It should not matter since the tasks are of the
// same type and hence should be identical.
func EnqueueIn(
	queue, jobType string,
	in time.Duration,
	args interface{},
	opts *Options,
) (string, error) {
	if opts == nil {
		opts = &Options{}
	}
	opts.At = workers.NowToSecondsWithNanoPrecision() + in.Seconds()

	return enqueueJobDesc(
		NewJobDesc(generateJid(), queue, jobType, opts),
		args,
	)
}

// Enqueue schedules the given task to the given queue. If a task of the same
// type is already scheduled, the given task will get scheduled anyway
// overriding the previous one.
func EnqueueForce(
	queue, jobType string,
	args interface{},
	opts *Options,
) (string, error) {
	return enqueueJobDesc(
		NewJobDesc(generateJid(), queue, jobType, opts),
		args,
		true,
	)
}

// Enqueue schedules the given task to the given queue with the given delay.
// If a task of the same type is already scheduled, the given task will get
// scheduled anyway overriding the previous one.
func EnqueueForceIn(
	queue, jobType string,
	in time.Duration,
	args interface{},
	opts *Options,
) (string, error) {
	if opts == nil {
		opts = &Options{}
	}
	opts.At = workers.NowToSecondsWithNanoPrecision() + in.Seconds()

	return enqueueJobDesc(
		NewJobDesc(generateJid(), queue, jobType, opts),
		args,
		true,
	)
}

func enqueueJobDesc(desc *JobDesc, args interface{}, override ...bool) (string, error) {
	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := workers.Config.Namespace + "once:q:" + desc.Queue + ":" + desc.JobType

	msg := workers.PrepareEnqueuMsg(desc.Queue, "", args,
		desc.Options.EnqueueOptions)
	msg.Set("jid", desc.Jid)
	msg.Set("x-once", desc)

	descJson, _ := msg.Get("x-once").MarshalJSON()
	if len(override) > 0 && override[0] {
		err := setNewJobDesc(conn, key, desc.Options.InitWaitTime, descJson)
		if err != nil {
			return "", err
		}
	} else {
		other, err := trySetNewDescJob(conn, key, desc.Options.InitWaitTime, descJson)
		if err != nil {
			return "", err
		} else if other != nil {
			return other.Jid, nil
		}
	}

	err := workers.EnqueueMsg(msg)
	if err != nil {
		unsetJobDesc(conn, key, desc.Jid)
		return "", err
	}

	return desc.Jid, nil
}

func setNewJobDesc(
	conn redis.Conn,
	key string,
	expire int,
	descJson []byte,
) error {
	_, err := redis.String(conn.Do("SET", key, descJson, "EX", expire))
	return err
}

func trySetNewDescJob(
	conn redis.Conn,
	key string,
	expire int,
	descJson []byte,
) (*JobDesc, error) {
	// Enqueue retry loop
	for {
		// Check if a job of the same type is already scheduled
		otherDescJson, err := redis.Bytes(conn.Do("GET", key))
		if err != nil && err != redis.ErrNil {
			return nil, err
		}

		// There is a scheduled job, inspect it
		if err == nil {
			otherDesc := JobDesc{}
			err := json.Unmarshal(otherDescJson, &otherDesc)
			// The job is still waiting to be executed for the first time,
			// return it
			if err == nil && otherDesc.IsInitWaiting() {
				return &otherDesc, err
			}

			// Either the job descriptor is bad, or it was already executed at
			// least once -- reschedule
			_, err = redis.String(conn.Do("SET", key, descJson, "EX", expire, "XX"))
			if err != nil && err != redis.ErrNil {
				return nil, err
			}

			if err == nil {
				return nil, nil
			}

			// retry
			continue
		}

		// No job is scheduled, try to add a new one
		_, err = redis.String(conn.Do("SET", key, descJson, "EX", expire, "NX"))
		if err != nil && err != redis.ErrNil {
			return nil, err
		}

		if err == nil {
			return nil, nil
		}

		// retry
	}
}

func unsetJobDesc(conn redis.Conn, key, jid string) error {
	// The script will set expires to 0 only if the JID value matches.
	_, err := updateJobStatus(conn, key, jid, "", 0)
	return err
}

func generateJid() string {
	return gocql.TimeUUID().String()
}

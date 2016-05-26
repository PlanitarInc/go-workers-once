package once

import (
	"encoding/json"

	"github.com/PlanitarInc/go-workers"
	"github.com/garyburd/redigo/redis"
	"github.com/gocql/gocql"
)

func Enqueue(
	queue, jobType string,
	args interface{},
	opts *Options,
) (string, error) {
	jid := generateJid()
	desc := NewJobDesc(jid, queue, jobType, opts)

	return enqueueJobDesc(desc, args)
}

func EnqueueForce(
	queue, jobType string,
	args interface{},
	opts *Options,
) (string, error) {
	jid := generateJid()
	desc := NewJobDesc(jid, queue, jobType, opts)

	return enqueueJobDesc(desc, args, true)
}

func enqueueJobDesc(desc *JobDesc, args interface{}, override ...bool) (string, error) {
	conn := workers.Config.Pool.Get()
	defer conn.Close()

	key := workers.Config.Namespace + "once:q:" + desc.Queue + ":" + desc.JobType

	msg := workers.PrepareEnqueuMsg(desc.Queue, "", args)
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

		// There is a scheduled job
		if err == nil {
			otherDesc := JobDesc{}
			err = json.Unmarshal(otherDescJson, &otherDesc)
			return &otherDesc, err
		}

		// No job is scheduled, try to enqueue
		_, err = redis.String(conn.Do("SET", key, descJson, "EX", expire, "NX"))
		if err != nil && err != redis.ErrNil {
			return nil, err
		}

		if err == nil {
			return nil, nil
		}
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

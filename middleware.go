package once

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/bitly/go-simplejson"
	"github.com/gomodule/redigo/redis"
)

type Middleware struct{}

func (r *Middleware) Call(
	queue string,
	message *workers.Msg,
	next func() bool,
) (acknowledge bool) {
	conn := workers.Config.Pool.Get()
	defer conn.Close()

	jobDesc, ok := message.CheckGet("x-once")
	if !ok {
		acknowledge = next()
		return
	}

	jid := message.Jid()
	jobType, _ := jobDesc.Get("job_type").String()
	cleanQueuename := strings.TrimPrefix(queue, workers.Config.Namespace)
	key := workers.Config.Namespace + "once:q:" + cleanQueuename + ":" + jobType
	opts := optionsFromJson(jobDesc.Get("options"))

	// XXX A hack to see whether a retry middleware is active and the job was
	// rescheduled: if the retry counter increased, the job was rescheduled.
	retryCount := r.getRetryCount(message)

	defer func() {
		if e := recover(); e != nil {
			newRetryCount := r.getRetryCount(message)
			if retryCount < newRetryCount {
				updateJobStatusWithResult(conn, key,
					jid, StatusRetryWaiting, opts.RetryTimeout, val2str(e))
			} else {
				updateJobStatusWithResult(conn, key,
					jid, StatusFailed, opts.FailureRetention, val2str(e))
			}

			panic(e)
		}
	}()

	n, _ := updateJobStatus(conn, key, jid, StatusExecuting, opts.ExecTimeout)
	if opts.AtMostOnce && n < 0 {
		// Two reasons for getting here, and they are NOT equivalent:
		//
		//  - (n=-2) another job of the same type has been scheduled after the
		//    current one. This job has been superseded; dropping it is the
		//    whole point of at-most-once.
		//
		//  - (n=-1) the job descriptor is gone from Redis, because its TTL
		//    elapsed. On a first attempt that does mean the job is lost to the
		//    outer world. On a RETRY it does not: go-workers deliberately
		//    rescheduled this job after a failure, and the descriptor has most
		//    likely expired precisely because the previous attempt outlived
		//    ExecTimeout. Dropping it here throws away work the queue
		//    correctly re-queued, with no error and no log.
		//
		// So a retry re-creates the descriptor and proceeds. Note what the
		// missing key means for a lock: nothing else is running, so this job is
		// free to go.
		// NOTE: getRetryCount returns -1 when the message
		// carries no retry_count at all (a fresh enqueue), and go-workers'
		// incrementRetry sets retry_count to *0* on the first failure -- not
		// 1. So the first retry, which is the case this fix exists for,
		// arrives with retryCount == 0.
		if n == -1 && retryCount >= 0 &&
			reclaimJobDesc(conn, key, jid, jobDesc, opts) {
		} else {
			acknowledge = true
			return
		}
	}

	acknowledge = next()
	updateJobStatus(conn, key, jid, StatusOK, opts.SuccessRetention)

	return
}

// reclaimJobDesc re-creates a job descriptor whose key expired while the job was
// executing, so a rescheduled retry can proceed instead of being dropped.
//
// The descriptor is rebuilt from the x-once snapshot carried on the message,
// which holds the original job_type, queue and options -- everything except the
// mutable status.
//
// SET ... NX is load-bearing: if a genuinely newer job of this type has claimed
// the key while we were away, NX fails and we return false so the caller drops
// this job. Without NX a retry could overwrite a newer job's descriptor and both
// would run, trading a dropped-retry bug for a duplicate-execution one -- which
// is worse, since at-most-once is the entire purpose of this middleware.
func reclaimJobDesc(
	conn redis.Conn,
	key, jid string,
	jobDesc *simplejson.Json,
	opts *Options,
) bool {
	nowMs := time2ms(time.Now())

	queue, _ := jobDesc.Get("queue").String()
	jobType, _ := jobDesc.Get("job_type").String()
	createdMs, err := jobDesc.Get("created_ms").Int64()
	if err != nil {
		createdMs = nowMs
	}

	desc := JobDesc{
		Jid:       jid,
		Status:    StatusExecuting,
		Queue:     queue,
		JobType:   jobType,
		CreatedMs: createdMs,
		UpdatedMs: nowMs,
		Options:   opts,
	}

	descJson, err := json.Marshal(&desc)
	if err != nil {
		return false
	}

	res, err := redis.String(conn.Do("SET", key, descJson,
		"NX", "EX", opts.ExecTimeout))
	if err != nil && err != redis.ErrNil {
		return false
	}

	return res == "OK"
}

func (r *Middleware) getRetryCount(message *workers.Msg) int {
	if val, err := message.Get("retry_count").Int(); err != nil {
		return -1
	} else {
		return val
	}
}

func val2str(val interface{}) string {
	switch v := val.(type) {
	case error:
		return v.Error()
	default:
		return fmt.Sprintf("%v", val)
	}
}

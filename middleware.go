package once

import (
	"strings"

	"github.com/PlanitarInc/go-workers"
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
				updateJobStatus(conn, key, jid, StatusRetryWaiting, opts.RetryWaitTime)
			} else {
				updateJobStatus(conn, key, jid, StatusFailed, opts.FailureRetention)
			}

			panic(e)
		}
	}()

	n, _ := updateJobStatus(conn, key, jid, StatusExecuting, opts.ExecWaitTime)
	if opts.AtMostOnce && n < 0 {
		// Two reasons for getting here:
		//  - (n=-1) the retention init/retry period of the job has elapsed,
		//    the job descriptor was removed from Redis;
		//  - (n=-2) another job of the same type has been scheduled after
		//    current one.
		// In both cases, the job is kind of lost for the outer world, so we
		// should silently drop it.
		acknowledge = true
		return
	}

	acknowledge = next()
	updateJobStatus(conn, key, jid, StatusOK, opts.SuccessRetention)

	return
}

func (r *Middleware) getRetryCount(message *workers.Msg) int {
	if val, err := message.Get("retry_count").Int(); err != nil {
		return -1
	} else {
		return val
	}
}

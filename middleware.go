package once

import "github.com/PlanitarInc/go-workers"

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
	key := workers.Config.Namespace + "once:q:" + queue + ":" + jobType
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

	updateJobStatus(conn, key, jid, StatusExecuting, opts.ExecWaitTime)
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

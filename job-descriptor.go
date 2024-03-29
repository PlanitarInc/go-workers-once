package once

import (
	"time"

	"github.com/PlanitarInc/go-workers"
	"github.com/bitly/go-simplejson"
)

const (
	StatusInitWaiting  string = "init-waiting"
	StatusExecuting           = "executing"
	StatusRetryWaiting        = "retry-waiting"
	StatusOK                  = "ok"
	StatusFailed              = "failed"
)

type JobDesc struct {
	Jid       string   `json:"jid"`
	Status    string   `json:"status"`
	Queue     string   `json:"queue"`
	JobType   string   `json:"job_type"`
	CreatedMs int64    `json:"created_ms"`
	UpdatedMs int64    `json:"updated_ms"`
	Options   *Options `json:"options"`
	Result    string   `json:"result"`
}

type Options struct {
	workers.EnqueueOptions
	AtMostOnce       bool `json:"at_most_once"`
	OverrideStarted  bool `json:"override_started"`
	InitWaitTime     int  `json:"init_wait"`
	RetryWaitTime    int  `json:"retry_wait"`
	ExecWaitTime     int  `json:"exec_wait"`
	SuccessRetention int  `json:"success_retention"`
	FailureRetention int  `json:"failure_retention"`
}

func optionsFromJson(obj *simplejson.Json) *Options {
	opts := Options{}

	opts.AtMostOnce, _ = obj.Get("at_most_once").Bool()
	opts.InitWaitTime, _ = obj.Get("init_wait").Int()
	opts.RetryWaitTime, _ = obj.Get("retry_wait").Int()
	opts.ExecWaitTime, _ = obj.Get("exec_wait").Int()
	opts.SuccessRetention, _ = obj.Get("success_retention").Int()
	opts.FailureRetention, _ = obj.Get("failure_retention").Int()

	return optionsMergeDefaults(&opts)
}

func optionsMergeDefaults(opts *Options) *Options {
	if opts == nil {
		opts = &Options{}
	}

	if opts.InitWaitTime == 0 {
		opts.InitWaitTime = 30
	}
	if opts.RetryWaitTime == 0 {
		opts.RetryWaitTime = 60
	}
	if opts.ExecWaitTime == 0 {
		opts.ExecWaitTime = 90
	}
	if opts.SuccessRetention == 0 {
		opts.SuccessRetention = 5
	}
	if opts.FailureRetention == 0 {
		opts.FailureRetention = 5
	}

	return opts
}

func NewJobDesc(jid, queue, jobType string, opts *Options) *JobDesc {
	nowMs := time2ms(time.Now())

	return &JobDesc{
		Jid:       jid,
		Status:    StatusInitWaiting,
		Queue:     queue,
		JobType:   jobType,
		CreatedMs: nowMs,
		UpdatedMs: nowMs,
		Options:   optionsMergeDefaults(opts),
	}
}

func (d JobDesc) CanBeOverridden() bool {
	// If OverrideStarted is set, we can override the task already started.
	// Otherwise we have to wait until the task is removed from Redis.
	return d.Options != nil && d.Options.OverrideStarted && d.Status != StatusInitWaiting
}

func (d JobDesc) IsInitWaiting() bool {
	return d.Status == StatusInitWaiting
}

func (d JobDesc) IsDone() bool {
	return d.Status == StatusOK || d.Status == StatusFailed
}

func (d JobDesc) IsFailed() bool {
	return d.Status == StatusFailed
}

func (d JobDesc) IsOK() bool {
	return d.Status == StatusOK
}

func (d JobDesc) CreatedAt() time.Time {
	return ms2time(d.CreatedMs)
}

func (d JobDesc) UpdatedAt() time.Time {
	return ms2time(d.UpdatedMs)
}

func time2ms(t time.Time) int64 {
	return t.UnixNano() / 1e6
}

func ms2time(ms int64) time.Time {
	return time.Unix(ms/1000, (ms%1000)*1e6)
}

package once

import (
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
	Jid     string   `json:"jid"`
	Status  string   `json:"status"`
	Queue   string   `json:"queue"`
	JobType string   `json:"job_type"`
	Options *Options `json:"options"`
}

type Options struct {
	workers.EnqueueOptions
	AtMostOnce       bool `json:"at_most_once"`
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
	return &JobDesc{
		Jid:     jid,
		Status:  StatusInitWaiting,
		Queue:   queue,
		JobType: jobType,
		Options: optionsMergeDefaults(opts),
	}
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

package once

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestOptionsMergeDefaults_Emtpy(t *testing.T) {
	RegisterTestingT(t)

	opts := optionsMergeDefaults(nil)
	Ω(opts).ShouldNot(BeNil())
	Ω(opts.InitWaitTime).Should(Equal(30))
	Ω(opts.RetryWaitTime).Should(Equal(60))
	Ω(opts.ExecWaitTime).Should(Equal(90))
	Ω(opts.SuccessRetention).Should(Equal(5))
	Ω(opts.FailureRetention).Should(Equal(5))

	opts = optionsMergeDefaults(&Options{})
	Ω(opts).ShouldNot(BeNil())
	Ω(opts.InitWaitTime).Should(Equal(30))
	Ω(opts.RetryWaitTime).Should(Equal(60))
	Ω(opts.ExecWaitTime).Should(Equal(90))
	Ω(opts.SuccessRetention).Should(Equal(5))
	Ω(opts.FailureRetention).Should(Equal(5))
}

func TestOptionsMergeDefaults_Full(t *testing.T) {
	RegisterTestingT(t)

	opts := optionsMergeDefaults(&Options{
		InitWaitTime:     11,
		RetryWaitTime:    87,
		ExecWaitTime:     -12,
		SuccessRetention: -2,
		FailureRetention: 1,
	})
	Ω(opts).ShouldNot(BeNil())
	Ω(opts.InitWaitTime).Should(Equal(11))
	Ω(opts.RetryWaitTime).Should(Equal(87))
	Ω(opts.ExecWaitTime).Should(Equal(-12))
	Ω(opts.SuccessRetention).Should(Equal(-2))
	Ω(opts.FailureRetention).Should(Equal(1))
}

func TestOptionsMergeDefaults_Partial(t *testing.T) {
	RegisterTestingT(t)

	opts := optionsMergeDefaults(&Options{
		InitWaitTime:     11,
		ExecWaitTime:     -12,
		FailureRetention: 1,
	})
	Ω(opts).ShouldNot(BeNil())
	Ω(opts.InitWaitTime).Should(Equal(11))
	Ω(opts.RetryWaitTime).Should(Equal(60))
	Ω(opts.ExecWaitTime).Should(Equal(-12))
	Ω(opts.SuccessRetention).Should(Equal(5))
	Ω(opts.FailureRetention).Should(Equal(1))
}

func TestNewJobDesc(t *testing.T) {
	RegisterTestingT(t)

	d := NewJobDesc("1", "ochered:2", "tipa-joba", nil)

	Ω(d.Jid).Should(Equal("1"))
	Ω(d.Queue).Should(Equal("ochered:2"))
	Ω(d.JobType).Should(Equal("tipa-joba"))

	nowMs := time2ms(time.Now())
	Ω(d.CreatedMs).Should(BeBetween(nowMs-100, nowMs+100))
	Ω(d.UpdatedMs).Should(Equal(d.CreatedMs))
}

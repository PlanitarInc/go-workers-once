package once

import (
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

func BeBetween(min, max int64) types.GomegaMatcher {
	return SatisfyAll(
		BeNumerically(">", min),
		BeNumerically("<", max),
	)
}

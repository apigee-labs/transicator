package storage

import (
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

func TestStorage(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("../test-reports/storage.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Storage suite", []Reporter{junitReporter})
}

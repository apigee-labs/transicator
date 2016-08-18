package storage

import (
	"flag"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var debugEnabled bool

func TestStorage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storage Suite")
}

var _ = BeforeSuite(func() {
	flag.Set("logtostderr", "true")
	if debugEnabled || os.Getenv("TESTDEBUG") != "" {
		flag.Set("v", "5")
	}
})
